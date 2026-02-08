"""Unit tests for transformers."""
import pytest
import pandas as pd

from src.transform.base import DataFrameTransformer


class TestDataFrameTransformer:
    """Tests for DataFrameTransformer."""
    
    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 2, 3],
            'name': ['alice', 'bob', 'bob', None],
            'value': [100, 200, 200, 300]
        })
    
    def test_drop_duplicates(self, sample_df):
        """Test duplicate removal."""
        transformer = DataFrameTransformer()
        transformer.drop_duplicates()
        
        result = transformer.transform(sample_df)
        
        assert len(result) == 3
    
    def test_drop_na(self, sample_df):
        """Test NA removal."""
        transformer = DataFrameTransformer()
        transformer.drop_na(subset=['name'])
        
        result = transformer.transform(sample_df)
        
        assert len(result) == 3
        assert result['name'].isna().sum() == 0
    
    def test_rename_columns(self, sample_df):
        """Test column renaming."""
        transformer = DataFrameTransformer()
        transformer.rename_columns({'id': 'user_id', 'name': 'user_name'})
        
        result = transformer.transform(sample_df)
        
        assert 'user_id' in result.columns
        assert 'user_name' in result.columns
    
    def test_chained_transformations(self, sample_df):
        """Test multiple chained transformations."""
        transformer = DataFrameTransformer()
        transformer \
            .drop_duplicates() \
            .drop_na(subset=['name']) \
            .rename_columns({'id': 'user_id'})
        
        result = transformer.transform(sample_df)
        
        assert len(result) == 2
        assert 'user_id' in result.columns
