import tushare as ts
from app.config import get_settings
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self):
        settings = get_settings()
        ts.set_token(settings.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        
    async def fetch_market_data(self):
        try:
            # 获取主要指数数据
            indices = ['000001.SH', '399001.SZ', '399006.SZ']  # 上证、深证、创业板
            today = datetime.now().strftime('%Y%m%d')
            
            df = self.pro.index_daily(
                ts_code=','.join(indices),
                trade_date=today,
                fields='ts_code,close,pct_chg,vol,amount'
            )
            
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            raise