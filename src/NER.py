import json
import os
import glob
import pandas as pd
from underthesea import ner
from rapidfuzz import process, fuzz
from tqdm import tqdm
from collections import Counter
from multiprocessing import Pool, cpu_count
import warnings
warnings.filterwarnings('ignore')

# --- CẤU HÌNH ---
INPUT_DIR = "data/interim"      
OUTPUT_DIR = "data/NER_processed"     
MAP_FILE = "data/ticker_map.json" 
NUM_WORKERS = max(1, cpu_count() - 1)  # Số CPU cores - 1

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. Load Ticker Map
def load_ticker_map():
    if not os.path.exists(MAP_FILE):
        print(f"❌ Lỗi: Không tìm thấy {MAP_FILE}. Hãy chạy src/build_ticker_map.py trước.")
        return {}
    with open(MAP_FILE, 'r', encoding='utf-8') as f:
        raw = json.load(f)
        # Giữ nguyên case gốc cho fuzzy matching
        return raw

TICKER_MAP = load_ticker_map()
print(f"Loaded {len(TICKER_MAP)} mappings from ticker_map.json")

# 2. Hàm trích xuất entities từ text (dùng NER)
def extract_companies(text):
    if not text: 
        return []
    
    try:
        tokens = ner(text)
    except:
        return []

    entities = []
    current_entity = []
    
    # Blacklist: Từ quá chung chung, không phải tên công ty
    blacklist = {
        'việt nam', 'hà nội', 'hồ chí minh', 'tp.hcm', 'sài gòn',
        'hoàn', 'phí', 'link', 'ngóng', 'ott', 'casa', 'việc',
        'big', 'top', 'vn-index', 'vnindex', 'hnx', 'upcom'
    }
    
    noise_keywords = ['ngày', 'tháng', 'năm', 'quý', 'mức', 'tỷ lệ', 'cuối', 'đầu', 'nửa', 'cột mốc']
    
    def is_valid_entity(text):
        """Kiểm tra xem entity có hợp lệ không"""
        if len(text) < 3:
            return False
        text_lower = text.lower()
        # Loại noise keywords
        if any(kw in text_lower for kw in noise_keywords):
            return False
        # Loại toàn số
        if text.replace(' ', '').replace('/', '').replace('-', '').replace('.', '').replace(',', '').isdigit():
            return False
        # Loại blacklist
        if text_lower.strip() in blacklist:
            return False
        return True
    
    for token in tokens:
        word, pos_tag, chunk_tag, ner_tag = token
        
        # Chỉ chấp nhận ORG, LOC, PER (có thể là tên công ty)
        if ner_tag in ['B-ORG', 'I-ORG', 'B-LOC', 'I-LOC', 'B-PER', 'I-PER']:
            current_entity.append(word)
        else:
            if current_entity:
                entity_text = " ".join(current_entity)
                if is_valid_entity(entity_text):
                    entities.append(entity_text)
                current_entity = []
    
    # Xử lý entity cuối cùng nếu còn sót
    if current_entity:
        entity_text = " ".join(current_entity)
        if is_valid_entity(entity_text):
            entities.append(entity_text)
    
    return entities

# 3. Hàm Map entities sang Tickers dùng Fuzzy Matching
def map_to_tickers(entities, target_ticker, threshold=90, debug=False):
    """
    Map các entities đã trích xuất sang ticker symbols
    
    Args:
        entities: Danh sách tên entities từ NER
        target_ticker: Ticker của bài báo đang xử lý (để loại trừ)
        threshold: Ngưỡng fuzzy matching (0-100), mặc định 90
    
    Returns:
        List các tickers liên quan (không bao gồm target_ticker)
    """
    found_tickers = set()
    
    # Aliases cho các ngân hàng/công ty lớn (tên viết tắt -> ticker)
    aliases = {
        # Ngân hàng Big 4
        'bidv': 'BID',
        'vietinbank': 'CTG',
        'vietcombank': 'VCB',
        'vcb': 'VCB',
        'agribank': 'None',  # Không niêm yết
        
        # Ngân hàng tư nhân lớn
        'techcombank': 'TCB',
        'mbbank': 'MBB',
        'mb': 'MBB',
        'vpbank': 'VPB',
        'acb': 'ACB',
        'á châu': 'ACB',
        'sacombank': 'STB',
        'sài gòn thương tín': 'STB',
        'stb': 'STB',
        'vib': 'VIB',
        'quốc tế': 'VIB',
        'tpbank': 'TPB',
        'tiên phong': 'TPB',
        'hdbank': 'HDB',
        'phát triển tp.hcm': 'HDB',
        'msb': 'MSB',
        'hàng hải': 'MSB',
        'lpb': 'LPB',
        'bưu điện liên việt': 'LPB',
        'liên việt': 'LPB',
        'seabank': 'SSB',
        'đông nam á': 'SSB',
        'ssb': 'SSB',
        'shb': 'SHB',
        'sài gòn - hà nội': 'SHB',
        'eximbank': 'EIB',
        'xuất nhập khẩu': 'EIB',
        'eib': 'EIB',
        'ocb': 'OCB',
        'phương đông': 'OCB',
        'vietcapitalbank': 'BVB',
        'bản việt': 'BVB',
        'bvb': 'BVB',
        'vietbank': 'VBB',
        'việt nam thương tín': 'VBB',
        'vbb': 'VBB',
        'abbank': 'ABB',
        'an bình': 'ABB',
        'ncb': 'NVB',
        'quốc dân': 'NVB',
        'navibank': 'NVB',
        'pvcombank': 'PVB',
        'đại chúng': 'PVB',
        'pgbank': 'PGB',
        'xăng dầu petrolimex': 'PGB',
        'kienlongbank': 'KLB',
        'kiên long': 'KLB',
        'klb': 'KLB',
        'baovietbank': 'BVB',
        'bảo việt': 'BVB',
        'vietabank': 'VAB',
        'việt á': 'VAB',
        'oceanbank': 'None',  # Đã sáp nhập vào VPBank
        'gpbank': 'GPB',
        'dầu khí toàn cầu': 'GPB',
        
        # Công ty chứng khoán
        'ssi': 'SSI',
        'chứng khoán sài gòn': 'SSI',
        'vci': 'VCI',
        'vietcap': 'VCI',
        'vcbs': 'None',  # Chứng khoán Vietcombank, không niêm yết
        'bsc': 'BVS',
        'bidv securities': 'BVS',
        'hsc': 'HCM',
        'thành phố hồ chí minh': 'HCM',
        'vps': 'VPS',
        'vndirect': 'VND',
        'vds': 'VDS',
        'fpts': 'FTS',
        'fpt securities': 'FTS',
        'bsi': 'BSI',
        'agriseco': 'AGR',
    }
    
    # Lấy danh sách tên công ty từ map
    company_names = list(TICKER_MAP.keys())
    all_tickers = set(TICKER_MAP.values())
    
    for entity in entities:
        entity_lower = entity.lower().strip()
        
        # Cách 0: Kiểm tra aliases trước
        if entity_lower in aliases:
            ticker = aliases[entity_lower]
            # Bỏ qua nếu ticker là None hoặc 'None' (không niêm yết)
            if ticker and ticker != 'None' and ticker != target_ticker:
                found_tickers.add(ticker)
                if debug:
                    print(f"    '{entity}' -> ALIAS {ticker}")
            continue
        
        # Cách 1: Exact match
        matched = False
        for comp_name, ticker in TICKER_MAP.items():
            if entity_lower == comp_name.lower():
                if ticker != target_ticker:
                    found_tickers.add(ticker)
                    if debug:
                        print(f"    '{entity}' -> '{comp_name}' (EXACT) -> {ticker}")
                matched = True
                break
        
        if matched:
            continue
            
        # Cách 2: Substring match (chỉ với tên dài)
        if len(entity) >= 10:
            for comp_name, ticker in TICKER_MAP.items():
                if len(comp_name) >= 15:
                    if comp_name.lower() in entity_lower or entity_lower in comp_name.lower():
                        if ticker != target_ticker:
                            found_tickers.add(ticker)
                            if debug:
                                print(f"    '{entity}' -> '{comp_name}' (SUBSTRING) -> {ticker}")
                            matched = True
                            break
        
        if matched:
            continue
        
        # Cách 3: Fuzzy matching
        match = process.extractOne(entity, company_names, scorer=fuzz.token_sort_ratio)
        
        if match:
            best_match_name, score, _ = match
            if debug and score >= 70:
                print(f"    '{entity}' -> '{best_match_name}' (fuzzy: {score:.1f})")
            if score >= threshold:
                ticker = TICKER_MAP[best_match_name]
                if ticker != target_ticker:
                    found_tickers.add(ticker)
        
        # Cách 4: Kiểm tra ticker trực tiếp
        if entity.upper() in all_tickers:
            ticker = entity.upper()
            if ticker != target_ticker:
                found_tickers.add(ticker)
                if debug:
                    print(f"    '{entity}' -> TICKER {ticker}")
                    
    return list(found_tickers)

# 4. Hàm xử lý một bài báo (worker function cho multiprocessing)
def process_single_article(args):
    """Xử lý một bài báo - chạy song song"""
    article, ticker_target = args
    
    full_text = f"{article.get('title', '')}. {article.get('content', '')}"
    
    # Trích xuất entities
    extracted_entities = extract_companies(full_text)
    
    # Map sang tickers
    related_tickers = map_to_tickers(extracted_entities, ticker_target, debug=False)
    
    # Lọc bỏ 'None'
    related_tickers = [t for t in related_tickers if t and t != 'None']
    
    article['related_tickers'] = ",".join(related_tickers)
    return article, related_tickers

# 5. Hàm xử lý file
def process_file(filepath):
    filename = os.path.basename(filepath)
    # File input dạng: VIC_clean.json -> lấy VIC
    ticker_target = filename.split('_')[0]
    
    print(f"\n🚀 Processing: {ticker_target}")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            articles = json.load(f)
    except Exception as e:
        print(f"  ❌ Lỗi đọc file: {e}")
        return
        
    if not articles:
        print(f"  ⚠️ File rỗng, bỏ qua.")
        return
    
    print(f"  📊 Total articles: {len(articles)}")
    print(f"  💻 Using {NUM_WORKERS} CPU workers...")
        
    final_data = []
    related_counter = Counter()
    
    # Chuẩn bị args cho multiprocessing
    args_list = [(article, ticker_target) for article in articles]
    
    # Xử lý song song với multiprocessing Pool
    with Pool(processes=NUM_WORKERS) as pool:
        results = list(tqdm(
            pool.imap(process_single_article, args_list, chunksize=20),
            total=len(articles),
            desc=f"Processing {ticker_target}",
            unit="article"
        ))
    
    # Thu thập kết quả
    for article, related_tickers in results:
        final_data.append(article)
        related_counter.update(related_tickers)
    
    print(f"\n  ✅ Unique tickers found: {len(related_counter)}")
        
    df = pd.DataFrame(final_data)
    if 'date' in df.columns:
        df = df.sort_values(by='date')
        
    try:
        out_csv = os.path.join(OUTPUT_DIR, f"{ticker_target}_final.csv")
        df.to_csv(out_csv, index=False, encoding='utf-8-sig')
        print(f"  ✅ Saved: {out_csv}")
    except Exception as e:
        print(f"  ❌ Lỗi lưu CSV: {e}")
    
    # Lưu Top Related
    top_10 = [t[0] for t in related_counter.most_common(10)]
    
    try:
        out_json = os.path.join(OUTPUT_DIR, f"{ticker_target}_relations.json")
        with open(out_json, 'w', encoding='utf-8') as f:
            json.dump({
                "target": ticker_target,
                "top_related": top_10,
                "stats": dict(related_counter.most_common(20))
            }, f, indent=4, ensure_ascii=False)
        print(f"  ✅ Top related: {top_10}\n")
    except Exception as e:
        print(f"  ❌ Lỗi lưu JSON: {e}\n")

if __name__ == "__main__":
    print(f"Loaded {len(TICKER_MAP)} mappings from ticker_map.json")
    print(f"🚀 Using {NUM_WORKERS} CPU workers for parallel processing\n")
    
    files = glob.glob(os.path.join(INPUT_DIR, "*_clean.json"))
    
    if not files:
        print(f"⚠️ Không tìm thấy file dữ liệu nào trong {INPUT_DIR}.")
    else:
        print(f"📁 Found {len(files)} files to process.\n")
        for f in files:
            try:
                process_file(f)
            except Exception as e:
                print(f"❌ Lỗi xử lý file {f}: {e}")
                import traceback
                traceback.print_exc()