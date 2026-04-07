from flask import Flask
import logging

# Flask configuration
FLASK_ENV = 'development'
FLASK_RUN_PORT = 5000

# Logging configuration
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOGGING_LEVEL = logging.DEBUG

# Exchange configuration constants
EXCHANGE_API_URL = 'https://api.example.com/exchange'
EXCHANGE_API_KEY = 'your_api_key_here'
