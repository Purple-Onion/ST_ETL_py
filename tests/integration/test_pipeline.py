"""Integration tests for ETL pipeline."""
import pytest
import pandas as pd
from pathlib import Path
import tempfile

from src.pipeline import ETLPipeline
from src.extract.csv_extractor import CSVExtractor
from src.transform.base import DataFrameTransformer
from src.load.csv_loader import CSVLoader


class TestETLPipeline:
    """Integration tests for complete ETL pipeline."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def sample_csv(self, temp_dir):
        """Create sample input CSV."""
        input_file = temp_dir / "input.csv"
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 4],
            'name': ['alice', 'bob', 'bob', 'charlie', None],
            'score': [85, 90, 90, 75, 95]
        })
        df.to_csv(input_file, index=False)
        return input_file
    
    def test_full_pipeline(self, temp_dir, sample_csv):
        """Test complete ETL pipeline execution."""
        output_file = temp_dir / "output.csv"
        
        # Configure pipeline
        pipeline = ETLPipeline(name="test_pipeline")
        
        # Add extractor
        pipeline.add_extractor(CSVExtractor(sample_csv))
        
        # Add transformer
        transformer = DataFrameTransformer()
        transformer \
            .drop_duplicates() \
            .drop_na(subset=['name']) \
            .select_columns(['id', 'name', 'score'])
        pipeline.add_transformer(transformer)
        
        # Add loader
        pipeline.add_loader(CSVLoader(output_file))
        
        # Run pipeline
        result = pipeline.run()
        
        # Verify output
        assert output_file.exists()
        output_df = pd.read_csv(output_file)
        assert len(output_df) == 3  # After removing duplicates and NA
        assert list(output_df.columns) == ['id', 'name', 'score']
