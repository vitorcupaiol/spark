"""
Configuration loader module

This module is responsible for loading configuration settings
based on the specified environment (dev, test, prod).
"""

import os
import json


def load_config(env=None):
    """
    Load configuration from JSON file based on environment

    Args:
        env (str): Environment name (dev, prod). Defaults to env var or 'dev'

    Returns:
        dict: Configuration for the specified environment
    """
    env = env or os.environ.get("ENV", "dev")

    config_path = os.path.join(os.path.dirname(__file__), "config.json")

    with open(config_path, "r") as file:
        config = json.load(file)

    if env not in config:
        raise ValueError(f"Environment '{env}' not found in config")

    return config[env]
