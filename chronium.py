import requests
import pandas as pd
import re
from datetime import datetime, timedelta
import pytz
import logging
from vnstock import Quote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VietstockAPICrawler:
    def __init__(self):
        self.api_url = "https://finance.vietstock.vn/data/CorpEventData"
        self.headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://finance.vietstock.vn/lich-su-kien.htm',
            'X-Requested-With': 'XMLHttpRequest',
        }

    def crawl_events(self, from_date, to_date, exchange=5, group=13, max_pages=10):
        all_data = []
        for page in range(1, max_pages+1):
            payload = {
                "fromDate": from_date,
                "toDate": to_date,
                "code": "",
                "catID": group,
                "exchangeID": exchange,
                "page": page,
                "pageSize": 50
            }
            logger.info(f"Fetching page {page} from API...")
            resp = requests.post(self.api_url, headers=self.headers, data=payload)
            if resp.status_code != 200:
                logger.warning(f"API error: {resp.status_code}")
                break
            result = resp.json()
            rows = result.get('data', [])
            if not rows:
                break
            for row in rows:
                # Extract dividend value from event content
                content = row.get('EventContent', '')
                match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*đồng/CP', content)
                if match:
                    row['dividendValue'] = int(match.group(1).replace(',', ''))
                else:
                    row['dividendValue'] = None
                all_data.append(row)
        return all_data

    def get_stock_price(self, stock_code, event_date):
        tz = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(tz)
        if now.hour < 9 or (now.hour == 9 and now.minute < 30):
            price_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            price_date = now.strftime('%Y-%m-%d')
        try:
            quote = Quote(symbol=stock_code, source='VCI')
            df = quote.history(start=price_date, end=price_date, interval='1D')
            if not df.empty:
                return df.iloc[0]['close'] or df.iloc[0]['high'] or df.iloc[0]['low']
            else:
                return 0
        except Exception as e:
            logger.warning(f"Không lấy được giá cho {stock_code} ngày {price_date}: {e}")
            return 0

    def add_stock_prices(self, data):
        tz = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(tz)
        min_date = now + timedelta(days=3)
        filtered_data = []
        for row in data:
            date_str = row.get('TradeDate') or row.get('NgayGDKHQ')
            try:
                dt = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
            except:
                dt = pd.NaT
            if pd.notnull(dt) and dt > min_date:
                stock_code = row.get('StockCode')
                if stock_code:
                    price = self.get_stock_price(stock_code, now.strftime('%Y-%m-%d'))
                    row['closePrice'] = price * 1000 if price else 0
                    if price and row.get('dividendValue'):
                        row['percent'] = int(round(row['dividendValue'] * 100 / (price * 1000)))
                    else:
                        row['percent'] = 0
                else:
                    row['closePrice'] = 0
                    row['percent'] = 0
                filtered_data.append(row)
        return filtered_data

if __name__ == "__main__":
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(tz)
    from_date = today.strftime('%Y-%m-%d')
    to_date = (today + timedelta(days=30)).strftime('%Y-%m-%d')
    crawler = VietstockAPICrawler()
    data = crawler.crawl_events(from_date, to_date, exchange=5, group=13, max_pages=3)
    logger.info(f"Fetched {len(data)} events from API")
    data = crawler.add_stock_prices(data)
    df = pd.DataFrame(data)
    df.to_csv("vietstock_api_events.csv", index=False, encoding='utf-8-sig')
    logger.info("Saved to vietstock_api_events.csv")
