"""CSV file extractor."""
import pandas as pd
from pathlib import Path
from typing import Any, Dict, Optional, Union
from .base import BaseExtractor


class CSVExtractor(BaseExtractor):
    """Extract data from CSV files."""
    
    def __init__(self, file_path: Union[str, Path], config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.file_path = Path(file_path)
        self._data = None
    
    def connect(self) -> None:
        """Verify file exists."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        self.logger.info(f"Connected to CSV file: {self.file_path}")
    
    def extract(self, **kwargs) -> pd.DataFrame:
        """Extract data from CSV file.
        
        Args:
            **kwargs: Additional arguments passed to pd.read_csv()
            
        Returns:
            pd.DataFrame: Extracted data
        """
        read_config = {**self.config, **kwargs}
        self._data = pd.read_csv(self.file_path, **read_config)
        self.logger.info(f"Extracted {len(self._data)} rows from {self.file_path}")
        return self._data
    
    def disconnect(self) -> None:
        """Clear data from memory."""
        self._data = None
        self.logger.info("Disconnected from CSV source")
