"""Base transformer class for ETL pipeline."""
from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for all transformers."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._transformations: List[Callable] = []
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Apply transformation to data."""
        pass
    
    def add_transformation(self, func: Callable) -> 'BaseTransformer':
        """Add a transformation function to the pipeline.
        
        Args:
            func: Transformation function
            
        Returns:
            self for method chaining
        """
        self._transformations.append(func)
        return self
    
    def apply_transformations(self, data: Any) -> Any:
        """Apply all registered transformations sequentially."""
        result = data
        for transform in self._transformations:
            result = transform(result)
            self.logger.debug(f"Applied transformation: {transform.__name__}")
        return result


class DataFrameTransformer(BaseTransformer):
    """Transformer for pandas DataFrames."""
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations to DataFrame.
        
        Args:
            data: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected DataFrame, got {type(data)}")
        
        result = self.apply_transformations(data)
        self.logger.info(f"Transformed DataFrame: {len(result)} rows")
        return result
    
    def drop_duplicates(self, subset: Optional[List[str]] = None) -> 'DataFrameTransformer':
        """Add duplicate removal transformation."""
        self.add_transformation(
            lambda df: df.drop_duplicates(subset=subset)
        )
        return self
    
    def drop_na(self, subset: Optional[List[str]] = None) -> 'DataFrameTransformer':
        """Add NA removal transformation."""
        self.add_transformation(
            lambda df: df.dropna(subset=subset)
        )
        return self
    
    def rename_columns(self, columns: dict) -> 'DataFrameTransformer':
        """Add column renaming transformation."""
        self.add_transformation(
            lambda df: df.rename(columns=columns)
        )
        return self
    
    def select_columns(self, columns: List[str]) -> 'DataFrameTransformer':
        """Add column selection transformation."""
        self.add_transformation(
            lambda df: df[columns]
        )
        return self
    
    def filter_rows(self, condition: Callable[[pd.DataFrame], pd.Series]) -> 'DataFrameTransformer':
        """Add row filtering transformation."""
        self.add_transformation(
            lambda df: df[condition(df)]
        )
        return self
