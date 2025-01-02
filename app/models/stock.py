from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from .base import Base
from datetime import datetime

class MarketData(Base):
    __tablename__ = "market_data"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime, default=datetime.now)
    index_code = Column(String(20))
    index_name = Column(String(50))
    close_price = Column(Float)
    change_pct = Column(Float)
    volume = Column(Float)
    amount = Column(Float)

class DailyReport(Base):
    __tablename__ = "daily_reports"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime, default=datetime.now)
    content = Column(Text)
    status = Column(String(20))
    created_at = Column(DateTime, default=datetime.now)