import pandas as pd
import numpy as np
import ta
import os

# --- CẤU HÌNH ---
MARKET_DATA_DIR = "data/market_data"       # Input 1: Giá (từ bước 1)
SENTIMENT_DIR = "data/sentiment"    # Input 2: Sentiment (từ bước LLM trước đó)
ALPHA_INPUT_DIR = "data/alpha_input"       # Output: Nguyên liệu cho Alpha
os.makedirs(ALPHA_INPUT_DIR, exist_ok=True)

# TARGET_TICKERS = ["FPT"]
TARGET_TICKERS = ["VIC", "FPT", "BID", "VNM", "VJC"]

def load_sentiment(ticker):
    """Đọc file sentiment và group theo ngày"""
    # Tìm file sentiment (ưu tiên file qwen72b)
    possible_files = [f"{ticker}_sentiment_qwen72b.csv", f"{ticker}_sentiment.csv"]
    path = None
    for f in possible_files:
        full_path = os.path.join(SENTIMENT_DIR, f)
        if os.path.exists(full_path):
            path = full_path
            break
            
    if not path:
        print(f"⚠️ Chưa có file Sentiment cho {ticker}")
        return None
        
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    # Tính trung bình điểm trong ngày
    return df.groupby('date')['target_score'].mean()

def process_features(ticker):
    print(f"\n🛠️ Đang chế biến đặc trưng cho {ticker}...")
    
    # 1. Đọc dữ liệu giá (Offline từ file CSV bước 1)
    price_path = os.path.join(MARKET_DATA_DIR, f"{ticker}_price.csv")
   
    
    df = pd.read_csv(price_path, parse_dates=['date'], index_col='date')
    
    # 2. Tính chỉ báo kỹ thuật (Technical Indicators)
    # Lợi suất
    df['returns'] = df['close'].pct_change()
    # VWAP (xấp xỉ)
    df['vwap'] = (df['high'] + df['low'] + df['close']) / 3
    # RSI 14
    df['rsi'] = ta.momentum.rsi(df['close'], window=14)
    # SMA
    df['sma_5'] = ta.trend.sma_indicator(df['close'], window=5)
    df['sma_20'] = ta.trend.sma_indicator(df['close'], window=20)
    # Bollinger Bands
    bb = ta.volatility.BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    # Volatility (Độ biến động)
    df['volatility'] = df['close'].rolling(20).std()
    
    # 3. Gộp dữ liệu Sentiment
    daily_sent = load_sentiment(ticker)
    
    if daily_sent is not None:
        # Merge vào DataFrame chính
        df = df.join(daily_sent.rename('sentiment_score'), how='left')
        
        # Fill NaN = giá trị ngày trước đó gần nhất (forward fill)
        df['sentiment_score'] = df['sentiment_score'].ffill().fillna(0)
        
        # Feature phái sinh từ Sentiment
        df['sentiment_diff'] = df['sentiment_score'].diff() # Thay đổi so với hôm qua
        df['sentiment_ma5'] = df['sentiment_score'].rolling(5).mean().fillna(0) # Xu hướng tuần
        
    # 4. Làm sạch (Xóa dòng NaN do tính chỉ báo ở mấy ngày đầu)
    df = df.dropna()
    
    # 5. Lưu kết quả
    out_path = os.path.join(ALPHA_INPUT_DIR, f"{ticker}_full_features.csv")
    df.to_csv(out_path)
    print(f"✅ Xong! File sẵn sàng cho LLM: {out_path}")
    print(f"   Các cột: {list(df.columns)}")

if __name__ == "__main__":
    for t in TARGET_TICKERS:
        process_features(t)