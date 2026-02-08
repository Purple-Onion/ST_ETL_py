"""Base loader class for ETL pipeline."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Abstract base class for all loaders."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to destination."""
        pass
    
    @abstractmethod
    def load(self, data: Any, **kwargs) -> None:
        """Load data to destination."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to destination."""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
