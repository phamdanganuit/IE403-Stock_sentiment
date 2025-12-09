import json
import os
import glob
import re
import pandas as pd
from tqdm import tqdm
from collections import Counter

# --- CẤU HÌNH ---
INPUT_DIR = "data/interim"      
OUTPUT_DIR = "data/processed"     
MAP_FILE = "data/ticker_map.json" 

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. Load Ticker Map và Tối ưu hóa cho tìm kiếm
def load_ticker_map():
    if not os.path.exists(MAP_FILE):
        print(f"❌ Lỗi: Không tìm thấy {MAP_FILE}.")
        return {}
    
    with open(MAP_FILE, 'r', encoding='utf-8') as f:
        raw_map = json.load(f)
        
    # Chuẩn bị map: Chuyển key về chữ thường để so khớp với văn bản
    # Sắp xếp key theo độ dài giảm dần (để ưu tiên bắt từ dài trước, tránh bắt nhầm từ ngắn)
    # Ví dụ: Ưu tiên bắt "Ngân hàng Tiên Phong" trước khi bắt "Tiên Phong"
    cleaned_map = {k.lower().strip(): v for k, v in raw_map.items()}
    sorted_keys = sorted(cleaned_map.keys(), key=len, reverse=True)
    
    return cleaned_map, sorted_keys

TICKER_MAP, SORTED_KEYS = load_ticker_map()

# 2. Hàm Quét Từ Điển (Thay thế cho NER)
def scan_tickers_from_text(text, target_ticker):
    if not text: return []
    
    # Chuyển văn bản về chữ thường để so sánh
    text_lower = text.lower()
    found_tickers = set()
    
    # Duyệt qua từng từ khóa trong từ điển
    # (Cách này hơi chậm nếu từ điển quá lớn, nhưng với 2000 key thì chạy vèo vèo)
    for key in SORTED_KEYS:
        # Bỏ qua các key quá ngắn (dưới 3 ký tự) để tránh nhiễu (trừ khi là mã 3 chữ cái)
        if len(key) < 3: 
            # Nếu key chính là mã chứng khoán (VD: "fpt", "vic") thì dùng regex word boundary để bắt chính xác
            # Tránh bắt "vic" trong từ "victory"
            if re.search(r'\b' + re.escape(key) + r'\b', text_lower):
                 if TICKER_MAP[key] != target_ticker:
                    found_tickers.add(TICKER_MAP[key])
            continue

        # Với các tên dài (VD: "vietcombank", "hòa phát"), dùng `in` là đủ nhanh
        if key in text_lower:
            # Map sang mã
            ticker = TICKER_MAP[key]
            if ticker != target_ticker: # Không tính chính mình là related
                found_tickers.add(ticker)
                
    return list(found_tickers)

# 3. Hàm xử lý file
def process_file(filepath):
    filename = os.path.basename(filepath)
    ticker_target = filename.split('_')[0] 
    
    print(f"🚀 Scanning keywords for: {ticker_target}...")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        articles = json.load(f)
        
    final_data = []
    related_counter = Counter()
    
    for article in tqdm(articles, desc=f"Scanning {ticker_target}"):
        # Lấy title và content
        title = article.get('title', '')
        content = article.get('content', '')
        
        scan_text = f"{title}. {content}"
        
        # --- BƯỚC QUAN TRỌNG: QUÉT TICKER ---
        related_tickers = scan_tickers_from_text(scan_text, ticker_target)
        
        related_counter.update(related_tickers)
        
        article['related_tickers'] = ",".join(related_tickers)
        final_data.append(article)
        
    # --- LƯU KẾT QUẢ ---
    df = pd.DataFrame(final_data)
    if 'date' in df.columns:
        df = df.sort_values(by='date')
        
    out_csv = os.path.join(OUTPUT_DIR, f"{ticker_target}_final.csv")
    df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    
    # Lưu Top Related
    top_10 = [t[0] for t in related_counter.most_common(10)]
    
    out_json = os.path.join(OUTPUT_DIR, f"{ticker_target}_relations.json")
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump({
            "target": ticker_target,
            "top_related": top_10,
            "stats": dict(related_counter.most_common(20))
        }, f, indent=4, ensure_ascii=False)
        
    print(f"   ✅ Done. Top related: {top_10}")

# 4. Main
if __name__ == "__main__":
    files = glob.glob(os.path.join(INPUT_DIR, "*_clean.json"))
    
    if not files:
        print(f"⚠️ Không tìm thấy file dữ liệu nào trong {INPUT_DIR}.")
    else:
        print(f"Tìm thấy {len(files)} file cần xử lý.")
        for f in files:
            process_file(f)