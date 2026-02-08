# ST_ETL_py

ETL (Extract, Transform, Load) pipeline framework in Python.

## Project Overview
- **Name**: ST_ETL_py
- **Goal**: Modular and extensible ETL pipeline implementation
- **Status**: 🚧 In development

## Project Structure
```
ST_ETL_py/
├── src/
│   ├── extract/           # Data extraction modules
│   │   ├── base.py        # BaseExtractor abstract class
│   │   ├── csv_extractor.py
│   │   └── api_extractor.py
│   ├── transform/         # Data transformation modules
│   │   └── base.py        # DataFrameTransformer with chaining
│   ├── load/              # Data loading modules
│   │   ├── base.py        # BaseLoader abstract class
│   │   ├── csv_loader.py
│   │   └── database_loader.py
│   ├── utils/             # Utility modules
│   │   ├── logging_config.py
│   │   └── config_loader.py
│   └── pipeline.py        # ETL orchestrator
├── tests/
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── config/
│   ├── config.json        # Default configuration
│   └── .env.example       # Environment variables template
├── data/                  # Data directory (gitignored)
│   ├── raw/
│   ├── processed/
│   └── staging/
└── logs/                  # Log files (gitignored)
```

## Installation

```bash
# Clone repository
git clone https://github.com/Purple-Onion/ST_ETL_py.git
cd ST_ETL_py

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e ".[dev]"
```

## Usage

### Basic Pipeline Example

```python
from src.pipeline import ETLPipeline
from src.extract.csv_extractor import CSVExtractor
from src.transform.base import DataFrameTransformer
from src.load.csv_loader import CSVLoader

# Configure pipeline
pipeline = ETLPipeline(name="my_pipeline")

# Add extractor
pipeline.add_extractor(CSVExtractor("data/raw/input.csv"))

# Add transformer with chained operations
transformer = DataFrameTransformer()
transformer \
    .drop_duplicates() \
    .drop_na(subset=['name']) \
    .rename_columns({'id': 'user_id'})
pipeline.add_transformer(transformer)

# Add loader
pipeline.add_loader(CSVLoader("data/processed/output.csv"))

# Run pipeline
result = pipeline.run()
```

### Custom Extractor

```python
from src.extract.base import BaseExtractor

class MyExtractor(BaseExtractor):
    def connect(self):
        # Establish connection
        pass
    
    def extract(self, **kwargs):
        # Extract data
        return data
    
    def disconnect(self):
        # Clean up
        pass
```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_extractors.py
```

## Components

### Extractors
- `CSVExtractor`: Extract data from CSV files
- `APIExtractor`: Extract data from REST APIs

### Transformers
- `DataFrameTransformer`: Chainable transformations for pandas DataFrames
  - `drop_duplicates()`, `drop_na()`, `rename_columns()`, `select_columns()`, `filter_rows()`

### Loaders
- `CSVLoader`: Load data to CSV files
- `DatabaseLoader`: Load data to SQL databases via SQLAlchemy

## Configuration

### Environment Variables
```bash
ETL_DATABASE_URL=postgresql://user:password@localhost:5432/db
ETL_API_KEY=your_api_key
ETL_LOG_LEVEL=INFO
```

### Config File (config/config.json)
```json
{
    "pipeline": {"name": "default_pipeline"},
    "extract": {"batch_size": 1000},
    "transform": {"drop_duplicates": true},
    "load": {"if_exists": "append"}
}
```

## License
MIT
