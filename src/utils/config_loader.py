"""Configuration loader for ETL pipeline."""
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


def load_config(
    config_path: Optional[str] = None,
    env_prefix: str = "ETL_"
) -> Dict[str, Any]:
    """Load configuration from file and environment variables.
    
    Environment variables override file configuration.
    
    Args:
        config_path: Path to JSON config file
        env_prefix: Prefix for environment variables
        
    Returns:
        Configuration dictionary
    """
    config: Dict[str, Any] = {}
    
    # Load from file if provided
    if config_path:
        path = Path(config_path)
        if path.exists():
            with open(path, 'r') as f:
                config = json.load(f)
    
    # Override with environment variables
    for key, value in os.environ.items():
        if key.startswith(env_prefix):
            config_key = key[len(env_prefix):].lower()
            config[config_key] = _parse_env_value(value)
    
    return config


def _parse_env_value(value: str) -> Any:
    """Parse environment variable value to appropriate type."""
    # Try to parse as JSON first (for complex types)
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        pass
    
    # Boolean
    if value.lower() in ('true', 'false'):
        return value.lower() == 'true'
    
    # Integer
    try:
        return int(value)
    except ValueError:
        pass
    
    # Float
    try:
        return float(value)
    except ValueError:
        pass
    
    # Return as string
    return value
