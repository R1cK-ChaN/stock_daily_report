from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from app.models import base, stock
from app.services.data_fetcher import DataFetcher
from app.services.llm_service import LLMService
from datetime import datetime
import logging

app = FastAPI()

# 初始化数据库
base.Base.metadata.create_all(bind=base.engine)

@app.post("/generate-report")
async def generate_report(db: Session = Depends(base.get_db)):
    try:
        # 1. 获取数据
        data_fetcher = DataFetcher()
        market_data = await data_fetcher.fetch_market_data()
        news_data = await data_fetcher.fetch_news()
        
        # 2. 保存市场数据
        for data in market_data:
            db_market_data = stock.MarketData(
                index_code=data['ts_code'],
                close_price=data['close'],
                change_pct=data['pct_chg'],
                volume=data['vol'],
                amount=data['amount']
            )
            db.add(db_market_data)
        
        # 3. 生成报告
        llm_service = LLMService()
        report_content = await llm_service.generate_report(market_data, news_data)
        
        # 4. 保存报告
        db_report = stock.DailyReport(
            content=report_content,
            status="completed"
        )
        db.add(db_report)
        db.commit()
        
        return {"status": "success", "report": report_content}
        
    except Exception as e:
        logging.error(f"Error generating report: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/reports/{date}")
async def get_report(date: str, db: Session = Depends(base.get_db)):
    try:
        report_date = datetime.strptime(date, "%Y-%m-%d")
        report = db.query(stock.DailyReport).filter(
            stock.DailyReport.date >= report_date,
            stock.DailyReport.date < report_date + timedelta(days=1)
        ).first()
        
        if report:
            return {"status": "success", "report": report.content}
        return {"status": "error", "message": "Report not found"}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}