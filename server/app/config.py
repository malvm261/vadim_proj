from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Сетевые настройки
    host: str = "0.0.0.0"
    port: int = 8000

    # Параметры задачи по умолчанию
    default_total_iterations: int = 10_000_000
    default_chunk_size: int = 500_000
    default_chunk_count: int = 20

    # Воркер считается оффлайн, если не было heartbeat дольше этого времени
    worker_timeout: int = 15


settings = Settings()
