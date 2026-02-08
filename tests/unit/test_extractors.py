"""Unit tests for extractors."""
import pytest
import pandas as pd
from pathlib import Path
import tempfile

from src.extract.csv_extractor import CSVExtractor


class TestCSVExtractor:
    """Tests for CSVExtractor."""
    
    def test_extract_valid_csv(self):
        """Test extraction from valid CSV file."""
        # Create temp CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,value\n")
            f.write("1,alice,100\n")
            f.write("2,bob,200\n")
            temp_path = f.name
        
        try:
            extractor = CSVExtractor(temp_path)
            with extractor:
                data = extractor.extract()
            
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 2
            assert list(data.columns) == ['id', 'name', 'value']
        finally:
            Path(temp_path).unlink()
    
    def test_extract_nonexistent_file(self):
        """Test extraction from nonexistent file raises error."""
        extractor = CSVExtractor("/nonexistent/file.csv")
        
        with pytest.raises(FileNotFoundError):
            extractor.connect()


class TestAPIExtractor:
    """Tests for APIExtractor."""
    
    def test_connection(self):
        """Test API connection initialization."""
        from src.extract.api_extractor import APIExtractor
        
        extractor = APIExtractor("https://api.example.com")
        extractor.connect()
        
        assert extractor.session is not None
        
        extractor.disconnect()
        assert extractor.session is None
