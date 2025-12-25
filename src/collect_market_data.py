import pandas as pd
from vnstock import Vnstock
from datetime import datetime
import os
import time

# --- CẤU HÌNH ---
# Danh sách các mã cần tải
TARGET_TICKERS = ["VIC", "FPT", "BID", "VNM", "VJC"] 
START_DATE = '2022-01-01'
OUTPUT_DIR = 'data/market_data'

# Tự động tạo thư mục nếu chưa có
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_stock_data_batch(ticker_list, start_date):
    print(f"🚀 BẮT ĐẦU TẢI DỮ LIỆU CHO {len(ticker_list)} MÃ CỔ PHIẾU...")
    print("-" * 50)
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    success_count = 0
    
    for ticker in ticker_list:
        print(f"🔄 Đang tải: {ticker}...", end=" ")
        
        output_file = os.path.join(OUTPUT_DIR, f"{ticker}_price.csv")
        
        try:
            # 1. Gọi API Vnstock (Thử nguồn VCI trước, nếu lỗi thì thử TCBS)
            try:
                stock = Vnstock().stock(symbol=ticker, source='VCI')
                df = stock.quote.history(start=start_date, end=end_date, interval='1D')
            except:
                print("(VCI lỗi, thử TCBS)...", end=" ")
                stock = Vnstock().stock(symbol=ticker, source='TCBS')
                df = stock.quote.history(start=start_date, end=end_date, interval='1D')

            # 2. Kiểm tra dữ liệu
            if df is not None and not df.empty:
                # --- CHUẨN HÓA DỮ LIỆU (QUAN TRỌNG CHO CÁC BƯỚC SAU) ---
                # Đổi tên cột về chữ thường (Close -> close, Time -> date)
                df.columns = [c.lower() for c in df.columns]
                
                # Nếu có cột 'time', đổi tên thành 'date' cho chuẩn
                if 'time' in df.columns:
                    df.rename(columns={'time': 'date'}, inplace=True)
                
                # Sắp xếp theo ngày tăng dần
                df.sort_values('date', inplace=True)

                # Lưu file
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"✅ OK! ({len(df)} dòng)")
                success_count += 1
            else:
                print("⚠️ Rỗng (Không có dữ liệu).")
                
        except Exception as e:
            print(f"❌ LỖI: {str(e)}")
        
        # Nghỉ 1 chút để tránh spam server (1 giây)
        time.sleep(1)

    print("-" * 50)
    print(f"🎉 HOÀN TẤT! Thành công {success_count}/{len(ticker_list)} mã.")
    print(f"📂 Kiểm tra thư mục: {os.path.abspath(OUTPUT_DIR)}")

if __name__ == "__main__":
    get_stock_data_batch(TARGET_TICKERS, START_DATE)