# Importer Artikel

Công cụ để import dữ liệu bài viết từ cơ sở dữ liệu Access sang các file CSV.

## Yêu cầu

- Python 3.7+
- Microsoft Access ODBC Driver
- Các thư viện Python (xem `requirements.txt`)

## Cài đặt

1. Tạo môi trường ảo (khuyến nghị):
   ```bash
   python -m venv venv
   .\venv\Scripts\activate
   ```

2. Cài đặt các thư viện cần thiết:
   ```bash
   pip install -r requirements.txt
   ```

3. Sao chép file cơ sở dữ liệu Access vào thư mục `data` hoặc cập nhật đường dẫn trong file `src/config.py`

## Sử dụng

Chạy chương trình:
```bash
python -m src.main
```

Các file CSV kết quả sẽ được lưu vào thư mục `data/output/`.

## Cấu trúc dự án

```
importer_artikel_project/
├── data/                   # Thư mục chứa dữ liệu
│   └── output/             # Các file CSV xuất ra
├── sql/                    # Các file SQL
├── src/                    # Mã nguồn
│   ├── __init__.py         # Khởi tạo package
│   ├── config.py           # Cấu hình
│   ├── database.py         # Kết nối cơ sở dữ liệu
│   └── main.py             # Điểm vào chương trình
├── requirements.txt        # Các thư viện phụ thuộc
└── README.md               # Tài liệu hướng dẫn
```

## Ghi chú

- Chương trình sẽ tạo các file CSV với mã hóa UTF-8 và dấu phân cách là dấu chấm phẩy (;)
- File log được lưu trong thư mục gốc với tên `importer.log`
