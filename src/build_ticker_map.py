import json
import re
from vnstock3 import Vnstock

def clean_company_name(name):
    
    if not isinstance(name, str): return ""
    
    prefixes = [
        "Công ty Cổ phần", "CTCP", "Tổng Công ty", "Tập đoàn", 
        "Công ty", "Doanh nghiệp", "Ngân hàng TMCP", "Ngân hàng"
    ]
    
    cleaned = name
    for prefix in prefixes:
        pattern = re.compile(re.escape(prefix), re.IGNORECASE)
        cleaned = pattern.sub("", cleaned)
    
    return cleaned.strip()

def build_fallback_ticker_map():
    print("Tao danh sach ticker mac dinh...")
    
    ticker_map = {
        "VIC": "VIC", "Vingroup": "VIC", "VinFast": "VIC",
        "VHM": "VHM", "Vinhomes": "VHM",
        "VRE": "VRE", "Vincom": "VRE",
        "VNM": "VNM", "Vinamilk": "VNM",
        "VJC": "VJC", "Vietjet": "VJC", "Vietjet Air": "VJC",
        "FPT": "FPT", "FPT Software": "FPT", "FPT Telecom": "FPT",
        "BID": "BID", "BIDV": "BID", "Ngân hàng BIDV": "BID",
        "VCB": "VCB", "Vietcombank": "VCB",
        "CTG": "CTG", "VietinBank": "CTG",
        "TCB": "TCB", "Techcombank": "TCB",
        "MBB": "MBB", "MBBank": "MBB",
        "VPB": "VPB", "VPBank": "VPB",
        "ACB": "ACB",
        "STB": "STB", "Sacombank": "STB",
        "MWG": "MWG", "Thế Giới Di Động": "MWG", "Bách Hóa Xanh": "MWG",
        "MSN": "MSN", "Masan": "MSN", "Masan Group": "MSN",
        "HPG": "HPG", "Hòa Phát": "HPG",
        "VGI": "VGI", "Viettel Global": "VGI",
        "FRT": "FRT", "Long Châu": "FRT",
        "GAS": "GAS", "PLX": "PLX", "VHC": "VHC",
        "SSI": "SSI", "HDB": "HDB", "POW": "POW",
    }
    
    output_path = "data/ticker_map.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ticker_map, f, ensure_ascii=False, indent=4)
    
    print(f"✅ Đã tạo {output_path} với {len(ticker_map)} từ khóa (fallback)!")

def build_full_ticker_map():
    print("Dang tai danh sach cong ty...")
    
    try:
        stock = Vnstock().stock(symbol='VIC', source='VCI')
        df = stock.listing.all_symbols()
    except Exception as e:
        print(f"Loi khi goi API VCI, thu nguon TCBS...")
        try:
            stock = Vnstock().stock(symbol='VIC', source='TCBS')
            df = stock.listing.all_symbols()
        except Exception as e2:
            print(f"Loi tat ca nguon: {e2}")
            print("Su dung danh sach mac dinh...")
            return build_fallback_ticker_map()

    ticker_map = {}
    
    print(f"   -> Tim thay {len(df)} ma co phieu.")
    print(f"   -> Cac cot: {df.columns.tolist()}")
    
    count_added = 0
    for _, row in df.iterrows():
        ticker = row.get('ticker', '') or row.get('symbol', '')
        if not ticker:
            continue
            
        full_name = row.get('organ_name', '') or row.get('organName', '') or row.get('companyName', '') or row.get('company', '')
        short_name = row.get('organ_short_name', '') or row.get('organShortName', '') or row.get('shortName', '')
        
        if short_name and short_name != ticker and len(short_name) > 1:
            ticker_map[short_name] = ticker
            count_added += 1
            
            core_name = clean_company_name(short_name)
            if len(core_name) > 2 and core_name != ticker and core_name != short_name: 
                ticker_map[core_name] = ticker
                count_added += 1

        if full_name and full_name != ticker and len(full_name) > 1:
            ticker_map[full_name] = ticker
            count_added += 1
            
            core_name_full = clean_company_name(full_name)
            if len(core_name_full) > 2 and core_name_full != ticker and core_name_full != full_name:
                ticker_map[core_name_full] = ticker
                count_added += 1
    
    print(f"   -> Da them {count_added} mapping tu API")

    custom_map = {
        "Vingroup": "VIC",
        "VinFast": "VIC",      
        "Vinhomes": "VHM",
        "Vincom": "VRE",
        "Vinamilk": "VNM",
        "Vietjet": "VJC",
        "Vietjet Air": "VJC",
        "Thế Giới Di Động": "MWG",
        "Bách Hóa Xanh": "MWG",
        "Điện Máy Xanh": "MWG",
        "Masan": "MSN",
        "Masan Group": "MSN",
        "WinMart": "MSN",
        "Hòa Phát": "HPG",
        "FPT Software": "FPT",
        "FPT Telecom": "FPT",
        "Long Châu": "FRT",
        "Viettel Global": "VGI",
        "Vietcombank": "VCB",
        "VietinBank": "CTG",
        "BIDV": "BID",
        "Techcombank": "TCB",
        "VPBank": "VPB",
        "MBBank": "MBB",
        "Sacombank": "STB",
        "Bamboo Airways": "BAV",
        "Thaco": "THACO"
    }
    
    print(f"   -> Them {len(custom_map)} custom mapping")
    ticker_map.update(custom_map)
    
    output_path = "data/ticker_map.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(ticker_map, f, ensure_ascii=False, indent=4)
        
    print(f"✅ Tao xong {output_path} voi {len(ticker_map)} tu khoa mapping!")

if __name__ == "__main__":
    build_full_ticker_map()