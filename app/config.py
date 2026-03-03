from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    serper_api_key: str = ""
    openai_api_key: str = ""
    supabase_url: str = ""
    supabase_key: str = ""
    supabase_jwt_secret: str = ""
    supabase_anon_key: str = ""

    # Rate limiting
    serper_max_rps: int = 50
    serper_max_concurrent: int = 20
    enricher_max_concurrent: int = 10
    enricher_domain_cooldown: float = 0.5

    # Scraping defaults
    batch_upsert_size: int = 50

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
