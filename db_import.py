import pandas as pd
import psycopg2
from psycopg2.extras import Json
import re
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

# --- CẤU HÌNH DATABASE ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "")
}

def parse_impressions(val):
    """Chuyển đổi text thành số nguyên bigint"""
    if pd.isna(val) or not str(val).strip():
        return 0
    val_str = str(val).upper().replace(',', '').replace('+', '').strip()
    try:
        if 'M' in val_str:
            return int(float(val_str.replace('M', '')) * 1000000)
        if 'K' in val_str:
            return int(float(val_str.replace('K', '')) * 1000)
        return int(float(val_str))
    except ValueError:
        return 0

def parse_run_duration(val):
    """Trích xuất con số từ '93Days', '1Day' -> 93, 1"""
    if pd.isna(val):
        return None
    match = re.search(r'\d+', str(val))
    return int(match.group()) if match else None

def parse_countries(val):
    """
    Tách chuỗi 'A/B/C' hoặc '["A/B/C"]' thành mảng ['A', 'B', 'C']
    Giữ nguyên tên quốc gia.
    """
    if pd.isna(val) or not str(val).strip():
        return []
        
    # 1. Dọn dẹp các ký tự thừa ở 2 đầu (ngoặc vuông, nháy kép, nháy đơn)
    val_str = str(val).strip('[]"\' ')
    
    # 2. Tách chuỗi thành list (ưu tiên dấu '/', nếu không có thì thử dấu ',')
    if '/' in val_str:
        raw_list = val_str.split('/')
    else:
        raw_list = val_str.split(',')
        
    # 3. Xóa khoảng trắng thừa ở mỗi tên quốc gia và loại bỏ các phần tử rỗng
    clean_names = [c.strip() for c in raw_list if c.strip()]
    
    return clean_names

def get_val(row, col_name, default=None):
    """Hàm helper để lấy giá trị từ Pandas Row, xử lý NaN thành None cho DB"""
    if col_name not in row or pd.isna(row[col_name]):
        return default
    val = row[col_name]
    # Xử lý chuỗi rỗng
    if isinstance(val, str) and not val.strip():
        return default
    return val

def import_excel_to_db(file_path):
    # Tự động nhận diện CSV hoặc Excel
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    # Lấy ngày hiện tại làm ngày crawl
    crawl_date = datetime.now().date()

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    success_count = 0
    error_count = 0

    try:
        for index, row in df.iterrows():
            ad_id_full = get_val(row, 'ad_id')
            if not ad_id_full:
                continue 

            app_id = get_val(row, 'app_id')
            network_name = get_val(row, 'network', 'Youtube')
            
            try:
                # 1. BẢNG NETWORKS
                cursor.execute("""
                    INSERT INTO networks (name) VALUES (%s)
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id;
                """, (network_name,))
                network_id = cursor.fetchone()[0]

                # 2. BẢNG CAMPAIGNS
                campaign_name = f"{network_name}/{app_id}" if app_id else network_name
                cursor.execute("""
                    INSERT INTO campaigns (network_id, external_campaign_id, name) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (network_id, external_campaign_id) WHERE external_campaign_id IS NOT NULL 
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING id;
                """, (network_id, app_id, campaign_name))
                campaign_id = cursor.fetchone()[0]

                cursor.execute("""
                    INSERT INTO ad_groups (campaign_id, name, gender_audience, age_audience)
                    VALUES (%s, 'WW_ALL_NS', 'ALL', 'NS')
                    ON CONFLICT (name) DO UPDATE SET 
                        campaign_id = EXCLUDED.campaign_id,
                        name = EXCLUDED.name
                    RETURNING id;
                """, (campaign_id,))
                ad_group_id = cursor.fetchone()[0]

                # 3. BẢNG TEXTS
                cursor.execute("""
                    INSERT INTO texts (
                        headline, headline_language, headline_translated, 
                        primary_text, primary_text_language, primary_text_translated
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """, (
                    get_val(row, 'headline'), get_val(row, 'headline_language'), get_val(row, 'headline_translated'),
                    get_val(row, 'description'), get_val(row, 'description_language'), get_val(row, 'description_translated')
                ))
                text_id = cursor.fetchone()[0]

                video_url = get_val(row, 'link_youtube') or get_val(row, 'original_post_link')
                
                cursor.execute("""
                    INSERT INTO videos (
                        video_url, transcript, transcript_translated, 
                        video_language
                    ) VALUES (%s, %s, %s, %s)
                    RETURNING id;
                """, (
                    video_url, 
                    get_val(row, 'transcript'), get_val(row, 'transcript_translated'),
                    get_val(row, 'transcript_language')
                ))
                video_id = cursor.fetchone()[0]

                countries = parse_countries(get_val(row, 'region'))
                
                top_1_pct = bool(get_val(row, 'top_1_percent_creative', False))
                top_10_pct = bool(get_val(row, 'top_10_percent_creative', False))
                run_duration = parse_run_duration(get_val(row, 'duration'))

                impression = parse_impressions(get_val(row, 'impression'))
                cursor.execute("""
                    INSERT INTO adsets (
                        data_source, ad_id_full, crawl_date,
                        ad_group_id, impression,
                        video_id, text_id, countries,
                        app_id, ad_network, original_post_link, ad_language,
                        start_date, end_date, ad_run_duration, 
                        top_1_pct_creative, top_10_pct_creative, filters_applied
                    ) VALUES (
                        'social_peta', %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, 
                        %s, %s, %s
                    )
                    ON CONFLICT (data_source, ad_id_full, crawl_date) 
                    DO UPDATE SET 
                        countries = EXCLUDED.countries,
                        app_id = EXCLUDED.app_id,
                        impression = EXCLUDED.impression,
                        ad_run_duration = EXCLUDED.ad_run_duration,
                        top_1_pct_creative = EXCLUDED.top_1_pct_creative,
                        top_10_pct_creative = EXCLUDED.top_10_pct_creative
                    RETURNING id;
                """, (
                    ad_id_full, crawl_date,
                    ad_group_id, impression,
                    video_id, text_id, Json(countries),
                    app_id, network_name, get_val(row, 'original_post_link'), get_val(row, 'language'),
                    get_val(row, 'start_date'), get_val(row, 'end_date'), run_duration,
                    top_1_pct, top_10_pct, get_val(row, 'filters_applied')
                ))
                
                success_count += 1

            except Exception as e:
                error_count += 1
                print(f"[!] Lỗi tại ad_id {ad_id_full}: {e}")
                conn.rollback()
                continue 

        conn.commit()
        print(f"\n--- TỔNG KẾT ---")
        print(f"Thành công: {success_count} dòng")
        print(f"Thất bại: {error_count} dòng")

    except Exception as e:
        print(f"Lỗi hệ thống nghiêm trọng: {e}")
    finally:
        cursor.close()
        conn.close()
