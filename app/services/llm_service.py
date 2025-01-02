import openai
from app.config import get_settings
import logging

logger = logging.getLogger(__name__)

class LLMService:
    def __init__(self):
        settings = get_settings()
        openai.api_key = settings.OPENAI_API_KEY
        
    async def generate_report(self, market_data, news_data):
        try:
            # 构建 prompt
            prompt = self._build_prompt(market_data, news_data)
            
            response = await openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "你是一个专业的股市分析师，负责撰写每日市场报告。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise
            
    def _build_prompt(self, market_data, news_data):
        prompt = f"""
请根据以下数据生成今日股市分析报告：

市场数据：
{self._format_market_data(market_data)}

重要新闻：
{self._format_news(news_data)}

请包含以下部分：
1. 市场概况
2. 板块分析
3. 重要新闻影响
4. 市场展望
"""
        return prompt
    
    def _format_market_data(self, market_data):
        return "\n".join([
            f"- {item['ts_code']}: 收盘价 {item['close']}, 涨跌幅 {item['pct_chg']}%, 成交量 {item['vol']}"
            for item in market_data
        ])
    
    def _format_news(self, news_data):
        return "\n".join([
            f"- {item['title']}"
            for item in news_data
        ])