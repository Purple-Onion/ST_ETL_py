"""CSV file loader."""
import pandas as pd
from pathlib import Path
from typing import Any, Dict, Optional, Union
from .base import BaseLoader


class CSVLoader(BaseLoader):
    """Load data to CSV files."""
    
    def __init__(self, output_path: Union[str, Path], config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.output_path = Path(output_path)
    
    def connect(self) -> None:
        """Ensure output directory exists."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Output directory ready: {self.output_path.parent}")
    
    def load(self, data: pd.DataFrame, **kwargs) -> None:
        """Load DataFrame to CSV file.
        
        Args:
            data: DataFrame to save
            **kwargs: Additional arguments passed to to_csv()
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected DataFrame, got {type(data)}")
        
        write_config = {'index': False, **self.config, **kwargs}
        data.to_csv(self.output_path, **write_config)
        self.logger.info(f"Loaded {len(data)} rows to {self.output_path}")
    
    def disconnect(self) -> None:
        """No cleanup needed for CSV."""
        self.logger.info("CSV loader disconnected")
