"""Simple configuration management for RAG Obsidian system."""

import yaml
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file has invalid YAML or missing keys
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(
            f"❌ {config_path} not found. Please create it from config.yaml.example"
        )
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate required keys
        required_keys = ['paths', 'models']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"❌ Missing required configuration section: {key}")
        
        # Validate required paths
        if 'vault_path' not in config['paths']:
            raise ValueError("❌ Missing required path: vault_path")
        
        # Set defaults
        config.setdefault('search', {})
        config['search'].setdefault('max_rdbms_results', 100)
        config['search'].setdefault('content_preview_length', 1000)
        
        config.setdefault('processing', {})
        config['processing'].setdefault('file_extensions', ['.md'])
        config['processing'].setdefault('recursive', True)
        config['processing'].setdefault('max_file_size_mb', 10)
        
        # Set metadata DB path default
        config['paths'].setdefault('metadata_db_path', './obsidian_metadata.db')
        
        # Set model defaults
        config['models'].setdefault('llm_timeout', 120.0)
        
        return config
        
    except yaml.YAMLError as e:
        raise ValueError(f"❌ Error parsing {config_path}: {e}")


def get_vault_path(config: Dict[str, Any]) -> str:
    """Get vault path from config."""
    return config['paths']['vault_path']


def get_db_path(config: Dict[str, Any]) -> str:
    """Get database path from config."""
    return config['paths']['metadata_db_path']


def get_llm_model(config: Dict[str, Any]) -> str:
    """Get LLM model from config."""
    return config['models']['llm_model']


def get_llm_timeout(config: Dict[str, Any]) -> float:
    """Get LLM timeout from config."""
    return config['models']['llm_timeout']


def get_max_results(config: Dict[str, Any]) -> int:
    """Get max results from config."""
    return config['search']['max_rdbms_results']


def get_content_preview_length(config: Dict[str, Any]) -> int:
    """Get content preview length from config."""
    return config['search']['content_preview_length']