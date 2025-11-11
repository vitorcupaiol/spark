# spark_factory.py
import os
import logging
from typing import Dict, Any, Callable, Optional
from pyspark.sql import SparkSession

class SparkFactory:
    """Production-grade Spark session factory"""
    
    _session = None
    _logger = logging.getLogger(__name__)
    
    @classmethod
    def get_session(cls, app_name: str = "SparkApp", config_path: str = None) -> SparkSession:
        """
        Get or create a SparkSession using config from YAML file.
        
        Args:
            app_name: Application name
            config_path: Path to YAML config (default: config/spark_config.yaml)
        """
        if cls._session is None:
            # Determine environment
            env = os.environ.get("SPARK_ENV", "dev")
            cls._logger.info(f"Creating session for: {env}")
            
            # Start building the session
            builder = SparkSession.builder.appName(f"{app_name}-{env}")
            
            # Load configs from YAML
            config_path = config_path or os.path.join("config", "spark_config.yaml")
            configs = cls._load_configs(config_path, env)
            
            # Apply all configs
            for key, value in configs.items():
                builder = builder.config(key, value)
                
            # Create the session
            cls._session = builder.getOrCreate()
            
        return cls._session
    
    @classmethod
    def _load_configs(cls, config_path: str, env: str) -> Dict[str, str]:
        """Load configurations from YAML file."""
        try:
            with open(config_path, 'r') as file:
                all_configs = yaml.safe_load(file)
            
            # Start with common configs
            configs = all_configs.get('common', {}).copy()
            
            # Apply environment-specific configs
            if env in all_configs:
                configs.update(all_configs[env])
            
            return configs
            
        except Exception as e:
            cls._logger.error(f"Error loading config: {str(e)}")
            # Return minimal defaults
            return {
                "spark.master": "local[*]" if env == "dev" else "yarn",
                "spark.sql.adaptive.enabled": "true"
            }
    
    @classmethod
    def run_job(cls, job_func: Callable[[SparkSession], Any], 
                app_name: str = "SparkJob", config_path: str = None) -> Any:
        """Run a Spark job with proper session management."""
        try:
            # Get session
            spark = cls.get_session(app_name, config_path)
            
            # Run the job
            result = job_func(spark)
            return result
            
        except Exception as e:
            cls._logger.error(f"Error in job: {str(e)}", exc_info=True)
            raise
        finally:
            # Keep session by default in prod environments
            pass
    
    @classmethod
    def stop_session(cls) -> None:
        """Explicitly stop the SparkSession."""
        if cls._session is not None:
            cls._logger.info("Stopping SparkSession")
            cls._session.stop()
            cls._session = None