# config/settings.py
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List
from enum import Enum
import os
from dotenv import load_dotenv

# Load .env file explicitly
load_dotenv()

class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StorageBackend(str, Enum):
    FILE = "file"
    DATABASE = "database"
    REDIS = "redis"


class Environment(str, Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class AzureSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AZURE_")
    
    subscription_id: str = Field(..., description="Azure subscription ID")
    tenant_id: str = Field(..., description="Azure tenant ID")
    client_id: Optional[str] = Field(None, description="Azure client ID for service principal")
    client_secret: Optional[str] = Field(None, description="Azure client secret")
    resource_group: Optional[str] = Field(None, description="Default resource group to scan")
    log_analytics_workspace_id: Optional[str] = Field(None, description="Log Analytics workspace ID")


class KubernetesSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="K8S_")
    
    kubeconfig_path: Optional[str] = Field(None, description="Path to kubeconfig file")
    namespace: str = Field("default", description="Default Kubernetes namespace")
    context: Optional[str] = Field(None, description="Kubernetes context to use")


class DiscoverySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DISCOVERY_")
    
    interval_hours: int = Field(24, description="Discovery interval in hours")
    parallel_workers: int = Field(5, description="Number of parallel workers")
    timeout_seconds: int = Field(300, description="Timeout for discovery operations")
    retry_attempts: int = Field(3, description="Number of retry attempts")
    retry_backoff_factor: float = Field(1.5, description="Backoff factor for retries")
    metrics_retention_days: int = Field(30, description="Metrics retention period")


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STORAGE_")
    
    backend: StorageBackend = Field(StorageBackend.FILE, description="Storage backend type")
    connection_string: Optional[str] = Field(None, description="Storage connection string")
    base_path: str = Field("./data", description="Base path for file storage")
    cache_ttl_seconds: int = Field(3600, description="Cache TTL in seconds")


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_")
    
    host: str = Field("0.0.0.0", description="API host")
    port: int = Field(8000, description="API port")
    workers: int = Field(1, description="Number of worker processes")
    reload: bool = Field(False, description="Enable auto-reload in development")
    access_log: bool = Field(True, description="Enable access logging")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    environment: Environment = Field(Environment.DEVELOPMENT, description="Environment")
    debug: bool = Field(False, description="Debug mode")
    log_level: LogLevel = Field(LogLevel.INFO, description="Log level")
    log_format: str = Field("json", description="Log format (json or text)")
    
    # Sub-settings - using model_validate to create instances
    azure: AzureSettings = Field(default_factory=lambda: AzureSettings())
    kubernetes: KubernetesSettings = Field(default_factory=lambda: KubernetesSettings())
    discovery: DiscoverySettings = Field(default_factory=lambda: DiscoverySettings())
    storage: StorageSettings = Field(default_factory=lambda: StorageSettings())
    api: APISettings = Field(default_factory=lambda: APISettings())

    @field_validator('environment', mode='before')
    @classmethod
    def validate_environment(cls, v):
        if isinstance(v, str):
            return Environment(v.lower())
        return v

    @field_validator('log_level', mode='before')
    @classmethod
    def validate_log_level(cls, v):
        if isinstance(v, str):
            return LogLevel(v.upper())
        return v

    @classmethod
    def create_from_env(cls) -> "Settings":
        """Create settings instance from environment variables."""
        return cls()