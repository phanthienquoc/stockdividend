import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VietStockScraper:
    def __init__(self, google_credentials_path=None):
        """
        Initialize VietStock scraper
        
        Args:
            google_credentials_path (str): Path to Google service account credentials JSON file
        """
        self.base_url = "https://finance.vietstock.vn"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Google Sheets setup
        if google_credentials_path:
            self.setup_google_sheets(google_credentials_path)
        else:
            self.gc = None
            logger.warning("No Google credentials provided. Will save to CSV instead.")
    
    def setup_google_sheets(self, credentials_path):
        """Setup Google Sheets connection"""
        try:
            scope = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']
            
            credentials = Credentials.from_service_account_file(
                credentials_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            logger.info("Google Sheets connection established successfully")
        except Exception as e:
            logger.error(f"Failed to setup Google Sheets: {str(e)}")
            self.gc = None
    
    def get_selenium_driver(self, headless=True):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument(f"--user-agent={self.headers['User-Agent']}")
            
            driver = webdriver.Chrome(options=chrome_options)
            return driver
        except Exception as e:
            logger.error(f"Failed to setup Selenium driver: {str(e)}")
            return None
    
    def scrape_with_selenium(self, url, wait_time=10):
        """Scrape data using Selenium (for JavaScript-heavy pages)"""
        driver = self.get_selenium_driver()
        if not driver:
            return None
        
        try:
            logger.info(f"Loading page: {url}")
            driver.get(url)
            
            # Wait for page to load
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Additional wait for dynamic content
            time.sleep(5)
            
            # Try to find data table
            data_elements = []
            
            # Look for common table selectors
            table_selectors = [
                "table",
                ".table",
                "[class*='table']",
                "[class*='data']",
                ".grid",
                "[class*='grid']"
            ]
            
            for selector in table_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        logger.info(f"Found {len(elements)} elements with selector: {selector}")
                        for element in elements:
                            if element.text.strip():
                                data_elements.append({
                                    'selector': selector,
                                    'text': element.text,
                                    'html': element.get_attribute('outerHTML')
                                })
                except Exception as e:
                    continue
            
            # Get page source for BeautifulSoup parsing
            page_source = driver.page_source
            
            return {
                'page_source': page_source,
                'data_elements': data_elements
            }
            
        except Exception as e:
            logger.error(f"Error scraping with Selenium: {str(e)}")
            return None
        finally:
            driver.quit()
    
    def scrape_with_requests(self, url):
        """Scrape data using requests (for simpler pages)"""
        try:
            session = requests.Session()
            session.headers.update(self.headers)
            
            response = session.get(url)
            response.raise_for_status()
            
            return response.text
        except Exception as e:
            logger.error(f"Error scraping with requests: {str(e)}")
            return None
    
    def parse_vietstock_data(self, html_content):
        """Parse VietStock HTML content, chỉ lấy bảng id='event-content'"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', id='event-content')
            data_list = []
            if not table:
                logger.warning("Không tìm thấy bảng với id='event-content'")
                return data_list

            rows = table.find_all('tr')
            if len(rows) < 2:
                return data_list

            # Extract headers
            header_row = rows[0]
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            if not headers:
                return data_list

           # Extract data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= len(headers):
                    row_data = {}
                    for i, cell in enumerate(cells[:len(headers)]):
                        row_data[headers[i] if i < len(headers) else f'Column_{i}'] = cell.get_text(strip=True)
                    # Trích số tiền cổ tức từ cột nội dung (giả sử tên là 'Nội dung')
                    content = row_data.get('Nội dung') or row_data.get('Event') or row_data.get('Sự kiện') or ''
                    match = re.search(r'(\\d+)\\s*đồng/CP', content)
                    if match:
                        row_data['Số tiền (đồng/CP)'] = int(match.group(1))
                    else:
                        row_data['Số tiền (đồng/CP)'] = None
                    data_list.append(row_data)
            return data_list
        except Exception as e:
            logger.error(f"Error parsing HTML: {str(e)}")
            return []
    
    def scrape_vietstock_events(self, from_date="2020-04-12", to_date="2099-06-12", 
                               exchange=5, group=13, max_pages=10):
        """
        Scrape VietStock corporate events data
        
        Args:
            from_date (str): Start date (YYYY-MM-DD)
            to_date (str): End date (YYYY-MM-DD)
            exchange (int): Exchange ID
            group (int): Group ID
            max_pages (int): Maximum pages to scrape
        """
        all_data = []
        
        for page in range(1, max_pages + 1):
            url = f"{self.base_url}/lich-su-kien.htm?from={from_date}&to={to_date}&tab=1&exchange={exchange}&page={page}&group={group}"
            
            logger.info(f"Scraping page {page}: {url}")
            
            # Try Selenium first (better for JavaScript-heavy sites)
            result = self.scrape_with_selenium(url)
            
            if result and result['page_source']:
                data = self.parse_vietstock_data(result['page_source'])
                
                if data:
                    all_data.extend(data)
                    logger.info(f"Found {len(data)} records on page {page}")
                else:
                    # Try requests as fallback
                    html_content = self.scrape_with_requests(url)
                    if html_content:
                        data = self.parse_vietstock_data(html_content)
                        if data:
                            all_data.extend(data)
                            logger.info(f"Found {len(data)} records on page {page} (fallback method)")
            
            # Break if no data found (likely reached end)
            if not data:
                logger.info(f"No data found on page {page}, stopping")
                break
            
            # Respectful delay
            time.sleep(2)
        
        return all_data
    
    def save_to_google_sheets(self, data, spreadsheet_name="VietStock_Events", worksheet_name="Events"):
        """Save data to Google Sheets"""
        if not self.gc:
            logger.error("Google Sheets not configured")
            return False
        
        try:
            if not data:
                logger.warning("No data to save")
                return False
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Try to open existing spreadsheet, create if doesn't exist
            try:
                spreadsheet = self.gc.open(spreadsheet_name)
                logger.info(f"Opened existing spreadsheet: {spreadsheet_name}")
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gc.create(spreadsheet_name)
                logger.info(f"Created new spreadsheet: {spreadsheet_name}")
            
            # Try to open existing worksheet, create if doesn't exist
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                # Clear existing data
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
            # Prepare data for upload
            values = [df.columns.tolist()] + df.values.tolist()
            
            # Upload data
            worksheet.update('A1', values)
            
            logger.info(f"Successfully saved {len(data)} records to Google Sheets")
            logger.info(f"Spreadsheet URL: {spreadsheet.url}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving to Google Sheets: {str(e)}")
            return False
    
    def save_to_csv(self, data, filename="vietstock_events.csv"):
        """Save data to CSV file as backup"""
        try:
            if not data:
                logger.warning("No data to save")
                return False
            
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"Data saved to CSV: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {str(e)}")
            return False

def main():
    """Main execution function"""
    # Configuration
    GOOGLE_CREDENTIALS_PATH = "account-credentials.json"  # Update this path
    
    # Initialize scraper
    scraper = VietStockScraper(GOOGLE_CREDENTIALS_PATH)
    
    # Scrape data
    logger.info("Starting VietStock data scraping...")
    from_date = datetime.today().strftime("%Y-%m-%d")
    to_date = (datetime.today() + timedelta(days=30)).strftime("%Y-%m-%d")
    data = scraper.scrape_vietstock_events(
        from_date=from_date,
        to_date=to_date,
        exchange=5,
        group=13,
        max_pages=1  # Adjust as needed
    )
    # data = scraper.scrape_vietstock_events(
    #     from_date="2020-04-12",
    #     to_date="2025-06-12",
    #     exchange=5,
    #     group=13,
    #     max_pages=1  # Adjust as needed
    # )
    
    if data:
        logger.info(f"Successfully scraped {len(data)} records")
        
        # Save to Google Sheets
        if scraper.gc:
            success = scraper.save_to_google_sheets(data, "VietStock_Events_2025")
            if not success:
                logger.warning("Failed to save to Google Sheets, saving to CSV instead")
                scraper.save_to_csv(data, "vietstock_events_backup.csv")
        else:
            # Save to CSV if Google Sheets not available
            scraper.save_to_csv(data, "vietstock_events.csv")
        
        # Display sample data
        if data:
            df = pd.DataFrame(data)
            print("\nSample data:")
            print(df.head())
            print(f"\nTotal columns: {len(df.columns)}")
            print(f"Columns: {list(df.columns)}")
            
    else:
        logger.error("No data was scraped")

if __name__ == "__main__":
    main()