import pandas as pd
import json
import os
from db_import import import_excel_to_db
from custom_logger import log

def json_to_excel(json_filepath: str) -> str:
    """Đọc file final_result JSON và xuất ra file Excel."""
    
    # 1. Định nghĩa schema chuẩn (các cột bắt buộc phải có)
    DEFAULT_COLUMNS = [
        "app_id", "filters_applied", "ad_id", "description", 
        "description_language", "description_translated", "duration", 
        "end_date", "headline", "headline_language", "headline_translated", 
        "impression", "language", "link_youtube", "network", 
        "original_post_link", "region", "start_date", 
        "top_10_percent_creative", "top_1_percent_creative", 
        "transcript", "transcript_language"
    ]
    
    # 2. Tạo thư mục crawl_results nếu chưa có
    output_dir = "crawl_results"
    os.makedirs(output_dir, exist_ok=True)
    
    log.info(f"Đang đọc dữ liệu từ file JSON: {json_filepath}")
    
    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        log.error(f"Không tìm thấy file JSON: {json_filepath}")
        return None

    run_id = data.get("run_id", "unknown")
    apps = data.get("apps", [])
    
    rows = []
    
    # 3. Bóc tách dữ liệu
    for app in apps:
        app_id = app.get("app_id")
        filters_applied = ", ".join(app.get("filters_applied", []))
        
        for ad in app.get("ads", []):
            gemini_data = ad.get("gemini_data")
            
            if not gemini_data:
                continue
                
            row = {
                "app_id": app_id,
                "filters_applied": filters_applied
            }
            row.update(gemini_data)
            rows.append(row)

    # 4. Tạo DataFrame với cơ chế giữ form
    if not rows:
        log.warning("Không có dữ liệu quảng cáo. Tạo file Excel với cấu trúc cột mặc định.")
        # Tạo DataFrame rỗng nhưng có sẵn các cột
        df = pd.DataFrame(columns=DEFAULT_COLUMNS)
    else:
        df = pd.DataFrame(rows)
        # Đảm bảo file Excel luôn đủ cột và đúng thứ tự, kể cả khi Gemini trả thiếu key
        df = df.reindex(columns=DEFAULT_COLUMNS)
    
    # 5. Xuất ra Excel
    excel_filename = f"excel_result_{run_id}.xlsx"
    excel_filepath = os.path.join(output_dir, excel_filename)
    
    df.to_excel(excel_filepath, index=False)
    log.info(f"Hoàn thành xuất Excel tại: {excel_filepath}")
    
    # 6. Chỉ đẩy vào Database nếu có dữ liệu thực tế
    if rows:
        log.info("Bắt đầu đẩy dữ liệu từ Excel vào Database (PostgreSQL)...")
        try:
            import_excel_to_db(excel_filepath)
        except Exception as e:
            log.error(f"Lỗi block import DB: {e}")
    else:
        log.info("File Excel trống, bỏ qua bước đẩy dữ liệu vào Database.")
    
    log.info("Hoàn thành toàn bộ Pipeline!")
    return excel_filepath


def export_page_id_excel(json_filepath: str) -> str:
    """Đọc file raw_bundle JSON và xuất ra file Excel chứa Page IDs theo từng cột."""
    output_dir = "crawl_results"
    os.makedirs(output_dir, exist_ok=True)
    
    log.info(f"Đang đọc dữ liệu từ file JSON: {json_filepath}")
    
    try:
        with open(json_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        log.error(f"Không tìm thấy file JSON: {json_filepath}")
        return None

    run_id = data.get("run_id", "unknown")
    apps = data.get("apps", [])
    
    export_dict = {}
    for app in apps:
        app_id = app.get("app_id")
        page_ids = [item["page_id"] for item in app.get("page_ids", []) if "page_id" in item]
        if page_ids:
            export_dict[app_id] = page_ids

    # Khởi tạo DataFrame phù hợp với tình trạng dữ liệu
    if not export_dict:
        log.warning("Không có Page ID nào hoặc mảng apps rỗng. Tạo file Excel trắng bóc.")
        df = pd.DataFrame()  # DataFrame hoàn toàn trống
    else:
        df = pd.DataFrame(dict([ (k, pd.Series(v)) for k, v in export_dict.items() ]))
    
    excel_filename = f"excel_result_{run_id}.xlsx"
    excel_filepath = os.path.join(output_dir, excel_filename)
    
    df.to_excel(excel_filepath, index=False)
    log.info(f"Hoàn thành xuất Excel Page IDs tại: {excel_filepath}")
    
    return excel_filepath