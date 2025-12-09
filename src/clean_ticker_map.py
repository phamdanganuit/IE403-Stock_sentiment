import json

def clean_ticker_map(input_file="data/ticker_map.json", output_file="data/ticker_map.json"):
    with open(input_file, 'r', encoding='utf-8') as f:
        ticker_map = json.load(f)
    
    print(f"Truoc khi clean: {len(ticker_map)} entries")
    
    seen_tickers = {}
    cleaned_map = {}
    duplicates = []
    
    for key, ticker in ticker_map.items():
        if ticker not in seen_tickers:
            seen_tickers[ticker] = key
            cleaned_map[key] = ticker
        else:
            duplicates.append(f"  - '{key}' -> '{ticker}' (da co '{seen_tickers[ticker]}' -> '{ticker}')")
    
    print(f"\nSau khi clean: {len(cleaned_map)} entries")
    print(f"Da xoa: {len(duplicates)} entries trung lap\n")
    
    if duplicates:
        print("Cac entry bi xoa:")
        for dup in duplicates[:20]:
            print(dup)
        if len(duplicates) > 20:
            print(f"  ... va {len(duplicates) - 20} entries khac")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cleaned_map, f, ensure_ascii=False, indent=4)
    
    print(f"\nDa luu file: {output_file}")

if __name__ == "__main__":
    clean_ticker_map()
