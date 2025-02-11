import os
import logging
from dotenv import load_dotenv

class EnvLoader:
    """Enterprise-wide Environment Variable Loader."""

    @staticmethod
    def load_env():
        """Loads environment variables based on the current environment."""

        # Get environment type (default to local)
        env = os.getenv("ENV", "local").lower()

        # Define environment-specific dotenv file
        dotenv_mapping = {
            "local": "config/.env.local",
            "local_docker": "config/.env.local.docker",
            "dev": "config/.env.dev",  # Fallback only, Kubernetes sets actual values
            "prod": "config/.env.prod"  # Fallback only, Kubernetes sets actual values
        }

        dotenv_file = dotenv_mapping.get(env)

        if dotenv_file and os.path.exists(dotenv_file):
            load_dotenv(dotenv_file)
            logging.info(f"Loaded environment variables from {dotenv_file}")
        else:
            logging.info(f"Skipping dotenv loading. Relying on OS environment variables.")

        # Validate required environment variables
        required_vars = ["POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "AWS_ACCESS_KEY", "AWS_SECRET_KEY"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {missing_vars}")

        return True  # Successfully loaded

EnvLoader.load_env()