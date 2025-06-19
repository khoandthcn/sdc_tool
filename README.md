# Hướng dẫn sử dụng Security Data Collector (SDC)

## 1. Giới thiệu

Security Data Collector (SDC) là một công cụ Python được thiết kế để thu thập sự kiện và cảnh báo từ các nguồn bảo mật như QRadar và Cortex XDR, sau đó ghi dữ liệu vào Hadoop HDFS hoặc file cục bộ.

## 2. Yêu cầu hệ thống

*   Python 3.8+ (môi trường phát triển và triển khai)
*   CentOS 7 (môi trường triển khai khuyến nghị)
*   OpenSSL 1.0.2 (yêu cầu cho môi trường triển khai)
*   Các thư viện Python:
    *   `qradar-api` (hoặc thư viện tương đương để tương tác với QRadar API)
    *   `cortex-xdr-client` (thư viện chính thức từ PyPI để tương tác với Cortex XDR API)
    *   `hdfs` (để ghi vào Hadoop HDFS)
    *   `wheel` (để đóng gói và cài đặt)

## 3. Cài đặt

### 3.1. Cài đặt từ file `.whl`

Sau khi nhận được file `security_data_collector-0.1.0-py3-none-any.whl`, bạn có thể cài đặt công cụ bằng `pip`:

```bash
pip3 install security_data_collector-0.1.0-py3-none-any.whl
```

### 3.2. Cài đặt từ mã nguồn (Môi trường phát triển)

1.  **Clone repository (nếu có) hoặc giải nén mã nguồn:**

    ```bash
    git clone <URL_repository>
    cd security_data_collector
    ```

2.  **Cài đặt các thư viện cần thiết:**

    ```bash
    pip3 install -r requirements.txt # (Nếu có file requirements.txt)
    # Hoặc cài đặt thủ công:
    pip3 install qradar-api cortex-xdr-client hdfs
    ```

## 4. Cấu hình (`config.ini`)

Công cụ SDC sử dụng file `config.ini` để định nghĩa pipeline thu thập dữ liệu và các thông số liên quan. Mỗi file `config.ini` sẽ định nghĩa **một pipeline duy nhất** theo mô hình `Source > Sink`.

Một file `config.ini.example` đã được cung cấp cùng với mã nguồn. Bạn nên sao chép và chỉnh sửa file này cho phù hợp với môi trường của mình.

```bash
cp config.ini.example config.ini
```

### 4.1. Cấu trúc file `config.ini`

File cấu hình được chia thành các section chính:

*   `[Pipeline]`: Định nghĩa nguồn và đích của pipeline.
*   `[General]`: Cấu hình chung như định dạng đầu ra, đường dẫn file trạng thái và log.
*   `[QRadar]`: Cấu hình cho nguồn QRadar (API hoặc Syslog).
*   `[CortexXDR]`: Cấu hình cho nguồn Cortex XDR API.
*   `[Hadoop]`: Cấu hình cho đích Hadoop HDFS.
*   `[LocalFile]`: Cấu hình cho đích file cục bộ (chủ yếu dùng để test).

**Ví dụ về cấu hình pipeline:**

```ini
pipeline = qradar > hdfs
```

Điều này có nghĩa là dữ liệu sẽ được thu thập từ QRadar và ghi vào HDFS.

### 4.2. Các thông số cấu hình quan trọng

*   **`[General]` Section:**
    *   `output_format`: Định dạng đầu ra của dữ liệu. Hiện tại chỉ hỗ trợ `json_gz`.
    *   `state_file_path`: Đường dẫn đến file JSON lưu trữ thời điểm thu thập cuối cùng của mỗi pipeline. Đảm bảo công cụ có quyền đọc/ghi vào file này.
    *   `log_file_path`: Đường dẫn đến file log của công cụ.
    *   `log_level`: Mức độ ghi log (DEBUG, INFO, WARNING, ERROR, CRITICAL).

*   **`[QRadar]` Section:**
    *   `qradar.initial_collection_timestamp`: Thời điểm bắt đầu thu thập dữ liệu nếu không tìm thấy trạng thái trước đó trong `state_file_path`.
    *   `qradar.input_type`: Loại input từ QRadar (`syslog`, `api_events`, `api_offenses`).
    *   **API Configuration (`qradar.api.host`, `qradar.api.token`, `qradar.api.aql_query_template_events`, `qradar.api.aql_query_template_offenses`):** Cấu hình kết nối và các template AQL query cho QRadar API. Sử dụng `{start_time}` và `{end_time}` làm placeholder.
    *   **Syslog Configuration (`qradar.syslog.protocol`, `qradar.syslog.port`, `qradar.syslog.bind_address`, `qradar.syslog.parser_type`):** Cấu hình cho Syslog Listener. `parser_type` hỗ trợ `raw`, `json`, `leef`. Đối với `leef`, có thể cấu hình thêm `leef_strict_parsing`, `leef_default_delimiter`, `leef_fallback_to_raw_on_error`.

*   **`[CortexXDR]` Section:**
    *   `cortex_xdr.initial_collection_timestamp`: Tương tự như QRadar.
    *   **API Configuration (`cortex_xdr.api.fqdn`, `cortex_xdr.api.key_id`, `cortex_xdr.api.key`, `cortex_xdr.api.xql_query_template_alerts`):** Cấu hình kết nối và template XQL query cho Cortex XDR API. Dữ liệu được truy vấn sẽ là luồng nén gzip và được chuyển trực tiếp đến sink mà không giải nén. Sử dụng `{start_time}` và `{end_time}` làm placeholder.

*   **`[Hadoop]` Section:**
    *   `hadoop.namenode_url`: URL của Hadoop NameNode.
    *   `hadoop.kerberos_enabled`: `True` để bật Kerberos, `False` để tắt.
    *   `hadoop.kerberos_principal`, `hadoop.keytab_path`: Thông tin Kerberos nếu được bật. **Lưu ý: Cần chạy `kinit -kt <keytab_path> <kerberos_principal>` trước khi chạy script.**
    *   `hadoop.hdfs_qradar_api_events_base_path`, `hadoop.hdfs_qradar_api_offenses_base_path`, `hadoop.hdfs_qradar_syslog_base_path`, `hadoop.hdfs_cortex_xdr_api_alerts_base_path`: Đường dẫn gốc trên HDFS cho từng loại dữ liệu. Dữ liệu sẽ được phân vùng theo ngày (`yyyyMMdd`).
    *   `hadoop.max_records_per_file`, `hadoop.max_file_size_mb`: Cấu hình chia nhỏ file trên HDFS.

*   **`[LocalFile]` Section:**
    *   `local_file.base_path`: Đường dẫn thư mục gốc để lưu file cục bộ.
    *   `local_file.max_records_per_file`, `local_file.max_file_size_mb`: Cấu hình chia nhỏ file cục bộ.

## 5. Cách chạy

Sau khi cài đặt và cấu hình file `config.ini`, bạn có thể chạy công cụ từ terminal:

```bash
sdc --config /path/to/your/config.ini
```

Nếu bạn không chỉ định `--config`, công cụ sẽ tìm file `config.ini` trong cùng thư mục với script hoặc sao chép từ `config.ini.example` nếu chưa có.

## 6. Triển khai dạng Service (Systemd)

Để chạy SDC như một dịch vụ nền trên hệ thống Linux (ví dụ CentOS 7), bạn có thể tạo một unit file Systemd mẫu như sau:

**Tạo file `/etc/systemd/system/sdc.service`:**

```ini
[Unit]
Description=Security Data Collector Service
After=network.target

[Service]
User=sdc_user # Thay bằng user mà công cụ sẽ chạy
Group=sdc_group # Thay bằng group mà công cụ sẽ chạy
WorkingDirectory=/opt/security_data_collector # Thay bằng đường dẫn cài đặt công cụ
ExecStart=/usr/bin/python3 /usr/local/bin/sdc --config /etc/security_data_collector/config.ini # Thay đường dẫn python và config nếu cần
Restart=always
StandardOutput=journal
StandardError=journal
SyslogIdentifier=sdc

[Install]
WantedBy=multi-user.target
```

**Lưu ý:**

*   Thay đổi `User`, `Group`, `WorkingDirectory`, và `ExecStart` cho phù hợp với môi trường của bạn.
*   Đảm bảo user `sdc_user` có quyền đọc/ghi vào `state_file_path` và `log_file_path` được cấu hình trong `config.ini`.
*   Nếu sử dụng Kerberos, đảm bảo `kinit` đã được chạy thành công với keytab của `sdc_user` trước khi dịch vụ khởi động, hoặc tích hợp `kinit` vào script khởi động dịch vụ.

**Sau khi tạo file service, chạy các lệnh sau:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable sdc.service
sudo systemctl start sdc.service
```

**Kiểm tra trạng thái dịch vụ:**

```bash
sudo systemctl status sdc.service
journalctl -u sdc.service -f
```

## 7. Môi trường phát triển

Để phát triển hoặc đóng góp cho SDC, bạn cần:

1.  Cài đặt Python 3.8+.
2.  Cài đặt `pip` và `venv`.
3.  Tạo và kích hoạt môi trường ảo:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

4.  Cài đặt các dependency:

    ```bash
    pip install -e .
    ```

5.  Chạy unit tests:

    ```bash
    python3 -m unittest discover tests
    ```

## 8. Đóng gói

Để đóng gói công cụ thành file `.whl`:

```bash
python3 setup.py bdist_wheel
```

File `.whl` sẽ được tạo trong thư mục `dist/`.

## 9. Cấu trúc mã nguồn

```
sdc_tool/
├── sdc_tool/
│   ├── __init__.py
│   ├── base_source.py      # Lớp trừu tượng cho các nguồn dữ liệu
│   ├── base_sink.py        # Lớp trừu tượng cho các đích dữ liệu
│   ├── qradar_source.py    # Triển khai nguồn QRadar
│   ├── cortex_xdr_source.py # Triển khai nguồn Cortex XDR
│   ├── local_file_sink.py  # Triển khai đích ghi file cục bộ
│   ├── hdfs_sink.py        # Triển khai đích ghi HDFS
│   ├── config_parser.py    # Xử lý đọc cấu hình từ config.ini
│   └── main.py             # Logic chính của công cụ và điều phối pipeline
├── tests/
│   ├── test_config_parser.py
│   ├── test_qradar_source.py
│   ├── test_cortex_xdr_source.py
│   ├── test_local_file_sink.py
│   └── test_hdfs_sink.py
├── docs/                   # Thư mục chứa tài liệu khác (nếu có)
├── config.ini.example      # File cấu hình mẫu
├── setup.py                # Cấu hình đóng gói Python
└── README.md               # Tài liệu này
```



