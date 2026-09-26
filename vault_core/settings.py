import os
from pathlib import Path

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class VaultSettings(BaseSettings):
        """Global configuration settings for Vault."""

        model_config = SettingsConfigDict(
            env_prefix="VAULT_",
            env_file=".env",
            extra="ignore"
        )

        env: str = "development"
        log_level: str = "INFO"
        secret_key: str = "prototype-insecure-shared-secret-change-in-prod"

        data_dir: Path = Path("./data")
        chunk_size_bytes: int = 8 * 1024 * 1024  # 8 MiB default chunk size

        host: str = "0.0.0.0"
        port: int = 8000
        metrics_enabled: bool = True

        retention_period_seconds: int = 86400  # 24 hours
        internal_auth_required: bool = True

except ImportError:
    from pydantic import BaseModel, Field

    class VaultSettings(BaseModel):  # type: ignore[no-redef]
        """Fallback settings using standard Pydantic BaseModel and environment variables."""

        env: str = Field(default_factory=lambda: os.getenv("VAULT_ENV", "development"))
        log_level: str = Field(default_factory=lambda: os.getenv("VAULT_LOG_LEVEL", "INFO"))
        secret_key: str = Field(default_factory=lambda: os.getenv("VAULT_SECRET_KEY", "prototype-insecure-shared-secret-change-in-prod"))

        data_dir: Path = Field(default_factory=lambda: Path(os.getenv("VAULT_DATA_DIR", "./data")))
        chunk_size_bytes: int = Field(default_factory=lambda: int(os.getenv("VAULT_CHUNK_SIZE_BYTES", str(8 * 1024 * 1024))))

        host: str = Field(default_factory=lambda: os.getenv("VAULT_HOST", "0.0.0.0"))
        port: int = Field(default_factory=lambda: int(os.getenv("VAULT_PORT", "8000")))
        metrics_enabled: bool = Field(default_factory=lambda: os.getenv("VAULT_METRICS_ENABLED", "true").lower() == "true")

        retention_period_seconds: int = Field(default_factory=lambda: int(os.getenv("VAULT_RETENTION_PERIOD_SECONDS", "86400")))
        internal_auth_required: bool = Field(default_factory=lambda: os.getenv("VAULT_INTERNAL_AUTH_REQUIRED", "true").lower() == "true")


# Default singleton instance
settings = VaultSettings()
