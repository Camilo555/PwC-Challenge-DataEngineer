"""
Centralized logging configuration.
Provides structured logging with support for multiple formats and outputs.
"""

import json
import logging
import logging.config
import sys
from collections.abc import MutableMapping
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any

from .config import settings


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "thread", "threadName", "exc_info", "exc_text", "stack_info",
                "getMessage", "message"
            ]:
                log_data[key] = value

        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output."""

    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.RESET}"
        return super().format(record)


def setup_logging(
    name: str,
    level: str | None = None,
    log_file: Path | None = None,
    use_json: bool | None = None,
) -> logging.Logger:
    """
    Set up logging for a module.

    Args:
        name: Logger name (usually __name__)
        level: Log level (default from settings)
        log_file: Log file path (default from settings)
        use_json: Use JSON format (default from settings)

    Returns:
        Configured logger instance
    """
    # If logging config file exists, use it once (root config), then return logger
    cfg_path = Path("config/logging.yaml")
    if cfg_path.exists():
        try:
            import yaml  # type: ignore

            config_dict = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
            logging.config.dictConfig(config_dict)
        except Exception:
            # Fall back to programmatic config below
            pass
        return logging.getLogger(name)

    logger = logging.getLogger(name)

    # Use settings defaults if not specified
    level = level or settings.log_level
    log_file = log_file or settings.log_file_path
    use_json = use_json if use_json is not None else (settings.log_format == "json")

    # Set log level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    console_formatter: JSONFormatter | logging.Formatter
    if use_json:
        console_formatter = JSONFormatter()
    else:
        if sys.stdout.isatty():  # Use colors if terminal
            console_formatter = ColoredFormatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        else:
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)

        file_formatter: JSONFormatter | logging.Formatter
        if use_json:
            file_formatter = JSONFormatter()
        else:
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


@cache
def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger with default configuration.
    Cached to avoid recreating loggers.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    return setup_logging(name)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Custom logger adapter for adding context to log messages.
    """

    def __init__(self, logger: logging.Logger, extra: dict[str, Any]) -> None:
        """Initialize logger adapter with extra context."""
        super().__init__(logger, extra)

    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        """Process log message and add extra context."""
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"].update(self.extra)
        return msg, kwargs


def get_logger_with_context(
    name: str,
    **context: Any
) -> LoggerAdapter:
    """
    Get a logger with additional context.

    Args:
        name: Logger name
        **context: Additional context to include in logs

    Returns:
        Logger adapter with context
    """
    logger = get_logger(name)
    return LoggerAdapter(logger, context)


# Create a default logger for the package
logger = get_logger(__name__)
