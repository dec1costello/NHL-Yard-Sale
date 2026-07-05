"""Logging utilities for the pipeline."""

import logging
import sys
from pathlib import Path
from typing import Optional

from src.config import get_config


def setup_logging(
    name: str = "nhl_ops",
    log_level: str = "INFO",
    log_file: Optional[Path] = None
) -> logging.Logger:
    """Setup structured logging for the pipeline."""
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Use UTF-8 encoding for console
    import io
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger."""
    config = get_config()
    log_file = config.logs_path / f"{name}.log"
    return setup_logging(name, log_file=log_file)
