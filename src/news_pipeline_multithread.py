import requests
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime, timedelta
from fake_useragent import UserAgent
import json
import re
import concurrent.futures
import os
import glob
import threading
from queue import Queue
from typing import List, Dict

def load_keywords_from_folder(keywords_folder="data/keywords"):
    keywords_data = []
    try:
        json_files = glob.glob(os.path.join(keywords_folder, "*.json"))
        for json_file in json_files:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                ticker = data.get("ticker", "")
                all_keywords = []
                all_keywords.extend(data.get("keywords_direct", []))
                all_keywords.extend(data.get("keywords_competitors", []))
                all_keywords.extend(data.get("keywords_macro", []))
                
                keywords_data.append({
                    "ticker": ticker,
                    "keywords": all_keywords
                })
                print(f"Loaded {len(all_keywords)} keywords for {ticker}")
        return keywords_data
    except Exception as e:
        print(f"Error loading keywords: {e}")
        return []

def load_config(config_file="config.txt"):
    config = {}
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key, value = key.strip(), value.strip()
                    if key in ["START_YEAR", "MAX_PAGES", "MAX_WORKERS"]:
                        config[key] = int(value)
                    elif key == "USE_PROXY":
                        config[key] = value.lower() == "true"
                    else:
                        config[key] = value
        return config
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def load_proxies(proxy_file):
    try:
        with open(proxy_file, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except: return []

class CrawlerEngine:
    def __init__(self, use_proxy=False, proxy_file=""):
        self.ua = UserAgent()
        self.use_proxy = use_proxy
        self.proxies = load_proxies(proxy_file) if use_proxy else []
        self.session_pool = Queue()
        for _ in range(20):
            self.session_pool.put(requests.Session())

    def get_session(self):
        return self.session_pool.get()

    def return_session(self, session):
        self.session_pool.put(session)

    def get_random_proxy(self):
        if not self.use_proxy or not self.proxies: return None
        p = random.choice(self.proxies)
        return {"http": f"http://{p}", "https": f"http://{p}"}

    def request(self, url, params=None, method="GET", headers=None, data=None, json_data=None):
        if not headers: headers = {}
        if 'User-Agent' not in headers: headers['User-Agent'] = self.ua.random
        
        session = self.get_session()
        try:
            time.sleep(random.uniform(0.5, 1.5))
            if method == "GET":
                response = session.get(url, params=params, headers=headers, proxies=self.get_random_proxy(), timeout=15)
            else:
                response = session.post(url, data=data, json=json_data, headers=headers, proxies=self.get_random_proxy(), timeout=15)
            return response if response.status_code == 200 else None
        except:
            return None
        finally:
            self.return_session(session)

class BaseSpider:
    def __init__(self, engine, config):
        self.engine = engine
        self.config = config
        self.source_name = "Base"
        self.crawled_data = []
        self.seen_urls = set()
        self.lock = threading.Lock()

    def parse_date_common(self, date_str):
        try:
            match = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})', date_str)
            if match:
                date_text = match.group(1).replace('-', '/')
                return datetime.strptime(date_text, "%d/%m/%Y")
        except: pass
        return None

    def add_item(self, title, url, pub_date, content, keyword):
        try:
            date_std = pub_date.strftime('%Y-%m-%d') if isinstance(pub_date, datetime) else str(pub_date)
        except: date_std = ""

        with self.lock:
            self.crawled_data.append({
                "source": self.source_name,
                "keyword": keyword,
                "title": title.strip(),
                "url": url,
                "published_date": date_std,
                "content": content.strip()
            })

    def is_url_seen(self, url):
        with self.lock:
            if url in self.seen_urls:
                return True
            self.seen_urls.add(url)
            return False

    def crawl(self, keyword): pass

class VnExpressSpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "VnExpress"

    def parse_vnexpress_date(self, raw):
        if not raw: return None
        raw = raw.replace("\xa0", " ").strip()
        if "," in raw:
            parts = raw.split(",")
            if len(parts) >= 2: raw = ",".join(parts[1:]).strip()
        if "(" in raw: raw = raw.split("(")[0].strip()

        date_formats = [
            "%d/%m/%Y, %H:%M", "%d/%m/%Y %H:%M", "%d/%m/%Y",
            "%d/%m/%Y, %H:%M:%S", "%d/%m/%Y %H:%M:%S"
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(raw, fmt)
            except: continue
        return None

    def crawl(self, keyword):
        base_url = "https://timkiem.vnexpress.net/?q={keyword}&page={page}"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(1, min(self.config["MAX_PAGES"] + 1, 6)):
                url = base_url.format(keyword=keyword, page=page)
                futures.append(executor.submit(self.crawl_page, url, keyword))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, url, keyword):
        res = self.engine.request(url)
        if not res: return

        soup = BeautifulSoup(res.content, 'html.parser')
        articles = soup.find_all("h3", class_="title-news")
        if not articles: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for item in articles:
                a_tag = item.find("a")
                if not a_tag: continue
                url_art = a_tag.get('href')
                
                if self.is_url_seen(url_art): continue

                time_tag = item.find_next("span", class_="time")
                if not time_tag and item.parent:
                    time_tag = item.parent.select_one(".time")
                
                if time_tag:
                    try:
                        d_str = time_tag.text.strip()
                        d_check = datetime.strptime(d_str, "%d/%m/%Y")
                        if d_check.year < self.config["START_YEAR"]:
                            return
                    except: pass

                article_futures.append(executor.submit(self.process, url_art, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, url, keyword):
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        
        date_tag = soup.find("span", class_="date")
        pub_date = self.parse_vnexpress_date(date_tag.text if date_tag else None)
        
        if pub_date and pub_date.year < self.config["START_YEAR"]:
            return
        
        title_tag = soup.find("h1", class_="title-detail")
        title = title_tag.text.strip() if title_tag else ""
        
        content_tag = soup.find("article", class_="fck_detail")
        content = ""
        if content_tag:
            content = " ".join([p.text.strip() for p in content_tag.find_all("p")])
        
        if title:
            self.add_item(title, url, pub_date if pub_date else datetime.now(), content, keyword)

class ThanhNienSpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "ThanhNien"
    
    def crawl(self, keyword):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(1, self.config["MAX_PAGES"] + 1):
                futures.append(executor.submit(self.crawl_page, keyword, page))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, keyword, page):
        api_url = f"https://thanhnien.vn/timelinesearch/{keyword}/{page}.htm"
        headers = {'X-Requested-With': 'XMLHttpRequest'}
        res = self.engine.request(api_url, params={"sort":0}, headers=headers)
        if not res: return
        
        soup = BeautifulSoup(res.text, 'html.parser')
        items = soup.select(".box-category-item")
        if not items: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for item in items:
                link = item.select_one("a.box-category-link-with-avatar")
                if not link: continue
                url = link.get('href')
                if not url.startswith('http'): url = "https://thanhnien.vn" + url
                if self.is_url_seen(url): continue
                
                time_tag = item.select_one(".box-time")
                if time_tag and time_tag.get('title'):
                    try:
                        dt = datetime.fromisoformat(time_tag.get('title'))
                        if dt.year < self.config["START_YEAR"]: return
                    except: pass
                
                article_futures.append(executor.submit(self.process, url, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, url, keyword):
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        title = soup.select_one("h1.detail-title span")
        title = title.text.strip() if title else ""
        
        content_div = soup.select_one("div.detail-content")
        if content_div:
            for t in content_div.select('.detail__related, .VCSortableInPreviewMode, script'): 
                t.decompose()
            content = content_div.get_text(separator="\n").strip()
        else: content = ""
        
        date_elem = soup.select_one("[data-role='publishdate']")
        dt = datetime.now()
        if date_elem:
             match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', date_elem.text)
             if match: dt = datetime.strptime(match.group(1), "%d/%m/%Y")
        
        self.add_item(title, url, dt, content, keyword)

class VnEconomySpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "VnEconomy"
    
    def crawl(self, keyword):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(1, self.config["MAX_PAGES"] + 1):
                futures.append(executor.submit(self.crawl_page, keyword, page))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, keyword, page):
        res = self.engine.request("https://vneconomy.vn/tim-kiem.html", params={"Text": keyword, "page": page})
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.select(".featured-row_item")
        if not items: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for item in items:
                link = item.select_one("a.link-layer-imt")
                if not link: continue
                url = "https://vneconomy.vn" + link.get('href') if link.get('href').startswith('/') else link.get('href')
                if self.is_url_seen(url): continue
                
                article_futures.append(executor.submit(self.process, url, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, url, keyword):
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        date_tag = soup.select_one(".date-detail .date")
        if date_tag:
            dt = self.parse_date_common(date_tag.text)
            if dt and dt.year < self.config["START_YEAR"]: return
        else: dt = datetime.now()
        
        title = soup.select_one("h1.name-detail")
        title = title.text.strip() if title else ""
        content = soup.select_one(".ct-edtior-web")
        content = content.get_text(separator="\n").strip() if content else ""
        self.add_item(title, url, dt, content, keyword)

class VietnamnetSpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "Vietnamnet"
    
    def crawl(self, keyword):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(0, self.config["MAX_PAGES"]):
                futures.append(executor.submit(self.crawl_page, keyword, page))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, keyword, page):
        url = f"https://vietnamnet.vn/tim-kiem-p{page}?q={keyword}&od=2"
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.select(".horizontalPost")
        if not items: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for item in items:
                link = item.select_one(".horizontalPost__main-title a")
                if not link: continue
                url_art = link.get('href')
                if url_art.startswith('/'): url_art = "https://vietnamnet.vn" + url_art
                if self.is_url_seen(url_art): continue
                
                article_futures.append(executor.submit(self.process, url_art, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, url, keyword):
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        date_tag = soup.select_one(".bread-crumb-detail__time")
        if not date_tag: date_tag = soup.select_one(".publish-date")
        if date_tag:
            dt = self.parse_date_common(date_tag.text)
            if dt and dt.year < self.config["START_YEAR"]: return
        else: dt = datetime.now()
        
        title = soup.select_one("h1.content-detail-title")
        title = title.text.strip() if title else ""
        content_tag = soup.select_one("#maincontent")
        content = ""
        if content_tag:
            for t in content_tag.select('table, script, .inner-article'): t.decompose()
            content = content_tag.get_text(separator="\n").strip()
        self.add_item(title, url, dt, content, keyword)

class CafeFSpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "CafeF"
    
    def crawl(self, keyword):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(1, self.config["MAX_PAGES"] + 1):
                futures.append(executor.submit(self.crawl_page, keyword, page))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, keyword, page):
        url = f"https://cafef.vn/tim-kiem.chn?keywords={keyword}&page={page}"
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.select(".timeline.list-bytags .item")
        if not items: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for item in items:
                h3 = item.find("h3")
                if not h3 or not h3.find("a"): continue
                link = h3.find("a").get('href')
                if not link.startswith('http'): link = "https://cafef.vn" + link
                if self.is_url_seen(link): continue
                
                article_futures.append(executor.submit(self.process, link, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, url, keyword):
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        date_tag = soup.find("span", class_="pdate")
        if date_tag:
            dt = self.parse_date_common(date_tag.text)
            if dt and dt.year < self.config["START_YEAR"]: return
        else: dt = datetime.now()
        
        title = soup.find("h1", class_="title")
        title = title.text.strip() if title else ""
        content = soup.find("div", class_="detail-content")
        content = content.get_text(separator="\n").strip() if content else ""
        self.add_item(title, url, dt, content, keyword)

class VietstockSpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "Vietstock"
    
    def crawl(self, keyword):
        res = self.engine.request(f"https://finance.vietstock.vn/{keyword}/tin-tuc-su-kien.htm")
        token = ""
        if res:
            s = BeautifulSoup(res.content, 'html.parser')
            inp = s.find("input", {"name": "__RequestVerificationToken"})
            if inp: token = inp['value']
        if not token: return

        headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': f"https://finance.vietstock.vn/{keyword}/tin-tuc-su-kien.htm"}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(1, self.config["MAX_PAGES"] + 1):
                futures.append(executor.submit(self.crawl_page, keyword, page, token, headers))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, keyword, page, token, headers):
        payload = {
            'view': '1', 'code': keyword, 'type': '1', 
            'fromDate': f"01/01/{self.config['START_YEAR']}", 
            'toDate': datetime.now().strftime("%d/%m/%Y"), 
            'channelID': '-1', 'page': str(page), 'pageSize': '20', 
            '__RequestVerificationToken': token
        }
        res = self.engine.request("https://finance.vietstock.vn/View/PagingNewsContent", data=payload, method="POST", headers=headers)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        rows = soup.find_all("tr")
        if not rows: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 2: continue
                link = cols[1].find("a")
                if not link: continue
                url = link.get('href')
                if url.startswith("//"): url = "https:" + url
                elif url.startswith("/"): url = "https://finance.vietstock.vn" + url
                if self.is_url_seen(url): continue
                
                date_str = cols[0].text.strip() 
                try:
                    dt = datetime.strptime(date_str.split()[0], "%d/%m/%y")
                    if dt.year < self.config["START_YEAR"]: return
                except: dt = datetime.now()
                
                article_futures.append(executor.submit(self.process, url, dt, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, url, dt, keyword):
        res = self.engine.request(url)
        if not res: return
        
        soup = BeautifulSoup(res.content, 'html.parser')
        title = soup.find("h1", class_="article-title")
        title = title.text.strip() if title else ""
        content_div = soup.find("div", id="vst_detail")
        content = content_div.get_text(separator="\n").strip() if content_div else ""
        self.add_item(title, url, dt, content, keyword)

class FireAntSpider(BaseSpider):
    def __init__(self, engine, config): 
        super().__init__(engine, config)
        self.source_name = "FireAnt"
    
    def crawl(self, keyword):
        res = self.engine.request(f"https://fireant.vn/ma-chung-khoan/{keyword}")
        token = None
        if res:
            soup = BeautifulSoup(res.content, 'html.parser')
            script = soup.find("script", id="__NEXT_DATA__")
            if script:
                data = json.loads(script.string)
                token = data.get('props', {}).get('pageProps', {}).get('initialState', {}).get('auth', {}).get('accessToken')
        if not token: return

        headers = {'Authorization': f"Bearer {token}"}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for page in range(0, self.config["MAX_PAGES"]):
                futures.append(executor.submit(self.crawl_page, keyword, page, headers))
            
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def crawl_page(self, keyword, page, headers):
        params = {'symbol': keyword, 'type': 1, 'limit': 20, 'offset': page * 20}
        res = self.engine.request("https://restv2.fireant.vn/posts", params=params, headers=headers)
        if not res: return
        
        posts = res.json()
        if not posts: return
        
        article_futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for post in posts:
                if not post.get('title'): continue
                post_id = post.get('postID')
                url = f"https://fireant.vn/bai-viet/{post_id}"
                if self.is_url_seen(url): continue
                
                date_str = post.get('date')
                try:
                    dt = datetime.fromisoformat(date_str)
                    if dt.year < self.config["START_YEAR"]: return
                except: dt = datetime.now()

                article_futures.append(executor.submit(self.process, post_id, post.get('title'), url, dt, headers, keyword))
            
            for future in concurrent.futures.as_completed(article_futures):
                future.result()

    def process(self, post_id, title, url, dt, headers, keyword):
        res = self.engine.request(f"https://restv2.fireant.vn/posts/{post_id}", headers=headers)
        content = ""
        if res:
            data = res.json()
            raw = data.get('content', '')
            if raw: content = BeautifulSoup(raw, 'html.parser').get_text(separator="\n").strip()
            else: content = data.get('description', '')
        self.add_item(title, url, dt, content, keyword)

class PipelineManager:
    def __init__(self):
        print("Initializing Multi-Thread Pipeline...")
        self.config = load_config()
        if not self.config: return
        
        self.keywords_data = load_keywords_from_folder(
            self.config.get("KEYWORDS_FOLDER", "data/keywords")
        )
        if not self.keywords_data:
            print("No keywords found!")
            return
        
        self.engine = CrawlerEngine(
            use_proxy=self.config.get("USE_PROXY", False), 
            proxy_file=self.config.get("PROXY_FILE", "")
        )
        self.spiders = [
            VnExpressSpider, ThanhNienSpider, VnEconomySpider, 
            VietnamnetSpider, CafeFSpider, VietstockSpider, FireAntSpider
        ]

    def run(self):
        print("="*50 + f"\nSTART CRAWL {len(self.keywords_data)} TICKERS\n" + "="*50)
        
        for ticker_data in self.keywords_data:
            ticker = ticker_data["ticker"]
            keywords = ticker_data["keywords"]
            
            print(f"\n{'='*50}")
            print(f"TICKER: {ticker} - {len(keywords)} keywords")
            print(f"{'='*50}")
            
            start_time = time.time()
            ticker_results = []
            
            max_workers = min(self.config["MAX_WORKERS"] * 2, len(keywords) * len(self.spiders))
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for keyword in keywords:
                    for SpiderClass in self.spiders:
                        futures.append(executor.submit(
                            self.run_spider, SpiderClass, keyword, ticker
                        ))
                
                completed = 0
                total = len(futures)
                for future in concurrent.futures.as_completed(futures):
                    data = future.result()
                    if data: 
                        ticker_results.extend(data)
                    completed += 1
                    if completed % 10 == 0:
                        print(f"Progress: {completed}/{total} tasks completed")
            
            elapsed = time.time() - start_time
            print(f"\nCompleted in {elapsed:.2f}s")
            
            self.save_results(ticker, ticker_results)

    def run_spider(self, SpiderClass, keyword, ticker):
        spider = SpiderClass(self.engine, self.config)
        try: 
            spider.crawl(keyword)
        except Exception as e: 
            print(f"Error {spider.source_name} - {keyword}: {e}")
        
        for item in spider.crawled_data:
            item["ticker"] = ticker
        
        return spider.crawled_data

    def save_results(self, ticker, results):
        results.sort(key=lambda x: x.get("published_date", ""), reverse=True)
        unique_results = {v['url']: v for v in results}.values()
        
        output_folder = self.config.get("OUTPUT_FOLDER", "data/output")
        os.makedirs(output_folder, exist_ok=True)
        
        output_file = os.path.join(output_folder, f"{ticker}_news.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(list(unique_results), f, ensure_ascii=False, indent=4)
        
        print(f"\n{ticker}: {len(unique_results)} articles -> {output_file}")

if __name__ == "__main__":
    PipelineManager().run()
