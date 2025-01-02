from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # Database
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    DB_NAME: str
    
    # Redis
    REDIS_HOST: str
    REDIS_PORT: int
    
    # API Keys
    TUSHARE_TOKEN: str
    OPENAI_API_KEY: str
    
    # Schedule
    REPORT_GENERATION_TIME: str
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()