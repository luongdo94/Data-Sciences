import os
import sys
import io
import pandas as pd
import pyodbc
from pathlib import Path

# Cấu hình lại stdout để hỗ trợ Unicode trong Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm thư mục gốc vào path để import các module
sys.path.append(str(Path(__file__).parent.parent))

from src.config import DATA_DIR, MDB_DATA
from src.database import read_csv_file

# Đọc dữ liệu từ file CSV
csv_path = os.path.join(DATA_DIR, "output", "comparison_results", "sku_differences.csv")
try:
    df = read_csv_file(csv_path)
    print(f"Da doc thanh cong file: {csv_path}")
    
    # In ra tất cả các cột để kiểm tra
    print("\nCac cot co trong file:")
    for col in df.columns:
        print(f"- {col}")
        
    # Kiểm tra xem có cột nào chứa 'aid_ew' không
    sku_columns = [col for col in df.columns if 'aid' in col.lower() or 'sku' in col.lower()]
    if not sku_columns:
        print("\nKhong tim thay cot nao chua ma SKU. Vui long kiem tra lai du lieu.")
        sys.exit(1)
        
    # Sử dụng cột đầu tiên chứa 'aid' hoặc 'sku' làm cột nguồn
    source_column = sku_columns[0]
    print(f"\nSu dung cot '{source_column}' de trich xuat ma mau")
    
except Exception as e:
    print(f"Loi khi doc file CSV: {e}")
    sys.exit(1)

def extract_color(sku):
    """Trích xuất mã màu từ chuỗi SKU."""
    if not isinstance(sku, str):
        return None
    
    parts = sku.split('-')
    if len(parts) >= 3:
        color_part = parts[1].split('/')[0].strip()
        return color_part.lower()
    return None

# Trích xuất mã màu từ cột nguồn
print("\nDang trich xuat ma mau tu du lieu...")
df['color'] = df[source_column].apply(extract_color)

# Kết nối đến cơ sở dữ liệu và lấy danh sách màu từ ERP
print("\nDang ket noi den co so du lieu...")
conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={MDB_DATA};"
try:
    with pyodbc.connect(conn_str) as conn:
        print("Ket noi thanh cong!")
        color_query = """
            SELECT a.ERP_Farben
            FROM tArtFarben AS a
            WHERE a.isExport = TRUE
        """
        print("Dang lay danh sach mau tu ERP...")
        erp_colors_df = pd.read_sql(color_query, conn)
        # Chuyển đổi cột ERP_Farben thành list và chuyển về chữ thường
        erp_colors = [str(color).lower() for color in erp_colors_df['ERP_Farben']]
        print(f"Da tai {len(erp_colors)} ma mau tu ERP")

except Exception as e:
    print(f"Loi khi ket noi hoac truy van co so du lieu: {e}")
    sys.exit(1)

# Hàm kiểm tra màu có trong danh sách ERP không
def check_color_in_erp(color):
    if pd.isna(color) or color is None:
        return False
    return color.lower() in erp_colors

# Thêm cột mới để kiểm tra
print("\nDang kiem tra cac ma mau...")
df['is_valid_color'] = df['color'].apply(check_color_in_erp)

# Tính toán thống kê
valid_count = df['is_valid_color'].sum()
invalid_count = (~df['is_valid_color']).sum()
total_count = len(df)

# Hiển thị kết quả
print("\n" + "="*50)
print("KET QUA KIEM TRA MA MAU")
print("="*50)
print(f"Tong so dong du lieu: {total_count:,}")
print(f"So luong ma mau hop le: {valid_count:,} ({valid_count/total_count:.1%})")
print(f"So luong ma mau khong hop le: {invalid_count:,} ({invalid_count/total_count:.1%})")

# Lưu kết quả ra file mới
output_dir = os.path.join(DATA_DIR, "output")
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "color_validation_results.csv")
try:
    df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
    print(f"\nDa luu ket qua vao: {output_file}")
except Exception as e:
    print(f"Loi khi luu file ket qua: {e}")
    import traceback
    traceback.print_exc()

if __name__ == "__main__":
    main()