"""Discovery orchestrator for coordinating multiple discovery services."""

import asyncio
from typing import List, Dict, Any, Optional
import structlog
from datetime import datetime, timezone

from finops.core.utils import gather_with_concurrency
from .base import BaseDiscoveryService

logger = structlog.get_logger(__name__)


class DiscoveryOrchestrator:
    """Orchestrates multiple discovery services."""
    
    def __init__(self, 
                 services: List[BaseDiscoveryService],
                 max_concurrency: int = 5,
                 timeout_seconds: Optional[int] = None):
        self.services = services
        self.max_concurrency = max_concurrency
        self.timeout_seconds = timeout_seconds
        self.logger = logger.bind(orchestrator="discovery")
    
    async def run_parallel_discovery(self) -> Dict[str, Any]:
        """Run all discovery services in parallel."""
        start_time = datetime.now(timezone.utc)
        
        self.logger.info(f"Starting discovery with {len(self.services)} services")
        
        # Create tasks for each service
        tasks = [
            self._run_service_with_timeout(service)
            for service in self.services
        ]
        
        # Execute with concurrency limit
        results = await gather_with_concurrency(
            tasks, 
            max_concurrency=self.max_concurrency,
            return_exceptions=True
        )
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        # Process results
        discovery_results = self._process_results(results)
        
        # Add orchestrator metadata
        discovery_results['orchestrator_metadata'] = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'total_services': len(self.services),
            'successful_services': sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'success'),
            'failed_services': sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'failed'),
            'max_concurrency': self.max_concurrency,
            'timeout_seconds': self.timeout_seconds
        }
        
        self.logger.info(
            f"Discovery completed in {duration:.2f}s",
            successful=discovery_results['orchestrator_metadata']['successful_services'],
            failed=discovery_results['orchestrator_metadata']['failed_services']
        )
        
        return discovery_results
    
    async def run_sequential_discovery(self) -> Dict[str, Any]:
        """Run discovery services sequentially."""
        start_time = datetime.now(timezone.utc)
        results = []
        
        self.logger.info(f"Starting sequential discovery with {len(self.services)} services")
        
        for service in self.services:
            try:
                result = await self._run_service_with_timeout(service)
                results.append(result)
                
                self.logger.info(
                    f"Completed discovery for {service.get_discovery_type()}",
                    status=result.get('status'),
                    resources=result.get('metadata', {}).get('resources_discovered', 0)
                )
                
            except Exception as e:
                error_result = {
                    'type': service.get_discovery_type(),
                    'status': 'failed',
                    'data': [],
                    'error': str(e),
                    'metadata': {'errors': [str(e)]}
                }
                results.append(error_result)
                self.logger.error(f"Discovery failed for {service.get_discovery_type()}", error=str(e))
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        discovery_results = self._process_results(results)
        discovery_results['orchestrator_metadata'] = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'total_services': len(self.services),
            'execution_mode': 'sequential'
        }
        
        return discovery_results
    
    async def _run_service_with_timeout(self, service: BaseDiscoveryService) -> Dict[str, Any]:
        """Run a single service with timeout."""
        try:
            if self.timeout_seconds:
                return await asyncio.wait_for(
                    service.discover_with_metadata(),
                    timeout=self.timeout_seconds
                )
            else:
                return await service.discover_with_metadata()
                
        except asyncio.TimeoutError:
            return {
                'type': service.get_discovery_type(),
                'status': 'timeout',
                'data': [],
                'error': f'Discovery timed out after {self.timeout_seconds} seconds',
                'metadata': {'errors': ['Timeout']}
            }
        except Exception as e:
            return {
                'type': service.get_discovery_type(),
                'status': 'failed',
                'data': [],
                'error': str(e),
                'metadata': {'errors': [str(e)]}
            }
    
    def _process_results(self, results: List[Any]) -> Dict[str, Any]:
        """Process orchestrator results."""
        discovery_data = {}
        summary = {
            'total_resources': 0,
            'successful_discoveries': 0,
            'failed_discoveries': 0,
            'discovery_types': [],
            'errors': []
        }
        
        for result in results:
            if isinstance(result, Exception):
                # Handle exceptions from gather
                summary['failed_discoveries'] += 1
                summary['errors'].append(str(result))
                continue
            
            if not isinstance(result, dict):
                summary['failed_discoveries'] += 1
                summary['errors'].append("Invalid result format")
                continue
            
            discovery_type = result.get('type', 'unknown')
            discovery_data[discovery_type] = result
            
            summary['discovery_types'].append(discovery_type)
            
            if result.get('status') == 'success':
                summary['successful_discoveries'] += 1
                resources_count = len(result.get('data', []))
                summary['total_resources'] += resources_count
            else:
                summary['failed_discoveries'] += 1
                if result.get('error'):
                    summary['errors'].append(f"{discovery_type}: {result['error']}")
        
        return {
            'discovery_data': discovery_data,
            'summary': summary
        }
