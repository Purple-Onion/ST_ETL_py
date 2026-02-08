"""Base extractor class for ETL pipeline."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for all extractors."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to data source."""
        pass
    
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Extract data from source."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to data source."""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
