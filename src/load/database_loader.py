"""Database loader using SQLAlchemy."""
import pandas as pd
from typing import Any, Dict, Optional
from .base import BaseLoader


class DatabaseLoader(BaseLoader):
    """Load data to SQL databases."""
    
    def __init__(
        self, 
        connection_string: str,
        table_name: str,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(config)
        self.connection_string = connection_string
        self.table_name = table_name
        self.engine = None
    
    def connect(self) -> None:
        """Create database engine."""
        try:
            from sqlalchemy import create_engine
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                pass
            self.logger.info(f"Connected to database")
        except ImportError:
            raise ImportError("SQLAlchemy required. Install with: pip install sqlalchemy")
    
    def load(
        self, 
        data: pd.DataFrame, 
        if_exists: str = 'append',
        **kwargs
    ) -> None:
        """Load DataFrame to database table.
        
        Args:
            data: DataFrame to load
            if_exists: How to handle existing table ('fail', 'replace', 'append')
            **kwargs: Additional arguments passed to to_sql()
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected DataFrame, got {type(data)}")
        
        if not self.engine:
            raise RuntimeError("Not connected. Call connect() first.")
        
        write_config = {
            'index': False,
            'if_exists': if_exists,
            **self.config,
            **kwargs
        }
        
        data.to_sql(self.table_name, self.engine, **write_config)
        self.logger.info(f"Loaded {len(data)} rows to table '{self.table_name}'")
    
    def disconnect(self) -> None:
        """Dispose database engine."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
        self.logger.info("Database connection closed")
