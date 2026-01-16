from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Worker configuration using Pydantic for environment variables.

    Reads from environment variables injected by Docker/Kubernetes.
    No .env file required - variables are read from os.environ.
    """

    model_config = SettingsConfigDict(
        env_file=".env",  # Optional: only used if .env exists (useful for local dev)
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Worker identification
    worker_id: str = Field(..., description="Unique worker identifier")

    # MongoDB configuration (unified)
    mongo_uri: str = Field(
        ..., description="Complete MongoDB connection URI including database"
    )

    # Azure Storage configuration
    azure_storage_connection_string: str = Field(
        ..., description="Azure Blob Storage connection string"
    )

    # Backend API configuration
    backend_url: str = Field(
        default="http://host.docker.internal:8000",
        description="Complete backend API URL",
    )
    api_secret: str = Field(..., description="Secret key for API authentication")

    # Processing configuration
    batch_size: int = Field(
        default=50_000, description="Number of records per batch for MongoDB insertion"
    )
    block_size_bytes: int = Field(
        default=16 * 1024 * 1024, description="Block size in bytes for CSV reading"
    )
    heartbeat_interval: int = Field(
        default=10, description="Heartbeat interval in seconds"
    )

    @property
    def mongo_database(self) -> str:
        """Extract database name from MongoDB URI."""
        # Parse database from URI like: mongodb://host:port/database or mongodb+srv://...
        if "/" in self.mongo_uri:
            parts = self.mongo_uri.rstrip("/").split("/")
            if len(parts) > 3:
                # Remove query params if any
                db_part = parts[-1].split("?")[0]
                if db_part:
                    return db_part
        raise ValueError(
            "Cannot extract database name from mongo_uri. Please include database in the URI."
        )


# Global settings instance
settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global settings instance."""
    global settings
    if settings is None:
        settings = Settings()
    return settings
