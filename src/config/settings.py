import os
import logging
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# API Keys
LLMWHISPERER_API_KEY = os.getenv("LLMWHISPERER_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Database

# Paths - objetos Path
PDF_BASE_PATH = Path(os.getenv("PDF_BASE_PATH", BASE_DIR / "data" / "pdfs"))
JSON_RAW_PATH = Path(os.getenv("JSON_RAW_PATH", BASE_DIR / "data" / "json_raw"))
JSON_TRANSFORMED_PATH = Path(os.getenv("JSON_TRANSFORMED_PATH", BASE_DIR / "data" / "json_transformed"))
LOG_PATH = Path(os.getenv("LOG_PATH", BASE_DIR / "logs"))

#URLs
LLMWHISPERER_BASE_URL = os.getenv("LLMWHISPERER_BASE_URL")


# Configure logging
def setup_logging():
    """Configure application logging"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(LOG_PATH / 'info.log'),
            logging.FileHandler(LOG_PATH / 'error.log'),
            logging.StreamHandler()
        ]
    )

    # Create separate log files for different levels
    error_handler = logging.FileHandler(LOG_PATH / 'error.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(log_format))

    warning_handler = logging.FileHandler(LOG_PATH / 'warning.log')
    warning_handler.setLevel(logging.WARNING)
    warning_handler.setFormatter(logging.Formatter(log_format))

    critical_handler = logging.FileHandler(LOG_PATH / 'critical.log')
    critical_handler.setLevel(logging.CRITICAL)
    critical_handler.setFormatter(logging.Formatter(log_format))

    # Add handlers to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(error_handler)
    root_logger.addHandler(warning_handler)
    root_logger.addHandler(critical_handler)

    return logging.getLogger(__name__)


# Initialize logging
logger = setup_logging()