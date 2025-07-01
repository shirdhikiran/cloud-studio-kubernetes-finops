"""Custom exceptions for the FinOps platform."""

from typing import Optional, Dict, Any


class FinOpsException(Exception):
    """Base exception for FinOps platform."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class DiscoveryException(FinOpsException):
    """Raised when discovery operations fail."""
    
    def __init__(self, discovery_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        self.discovery_type = discovery_type
        super().__init__(f"Discovery failed for {discovery_type}: {message}", details)


class ClientConnectionException(FinOpsException):
    """Raised when client connections fail."""
    
    def __init__(self, client_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        self.client_type = client_type
        super().__init__(f"{client_type} connection failed: {message}", details)


class DataValidationException(FinOpsException):
    """Raised when data validation fails."""
    
    def __init__(self, field: str, value: Any, message: str):
        self.field = field
        self.value = value
        super().__init__(f"Validation failed for {field}: {message}")


class ConfigurationException(FinOpsException):
    """Raised when configuration is invalid."""
    pass


class StorageException(FinOpsException):
    """Raised when storage operations fail."""
    pass


class MetricsException(FinOpsException):
    """Raised when metrics collection fails."""
    pass