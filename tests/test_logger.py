"""
Unit tests for src/utils/logger.py

Verifies that the shared application logger is configured correctly:
name, level, handler type, and singleton behavior.
"""
import logging
from utils.logger import get_logger


def test_get_logger_returns_logger():
    """Should return a standard logging.Logger instance."""
    logger = get_logger()
    assert isinstance(logger, logging.Logger)


def test_logger_name():
    """Logger name must match the project convention."""
    logger = get_logger()
    assert logger.name == "maven_logger"


def test_logger_level():
    """Default level should be INFO for production logging."""
    logger = get_logger()
    assert logger.level == logging.INFO


def test_logger_has_handler():
    """At least one handler must be attached."""
    logger = get_logger()
    assert len(logger.handlers) >= 1


def test_logger_handler_is_stream():
    """Output should go to a StreamHandler (console)."""
    logger = get_logger()
    stream_handlers = [
        h for h in logger.handlers
        if isinstance(h, logging.StreamHandler)
    ]
    assert len(stream_handlers) >= 1


def test_logger_singleton():
    """Calling get_logger() twice should return the same instance."""
    logger1 = get_logger()
    logger2 = get_logger()
    assert logger1 is logger2

    
