#!/usr/bin/env python3
import argparse
import json
import os
import time
import requests
import psycopg2
from datetime import datetime
from typing import Optional
import subprocess
import uuid

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from bs4 import BeautifulSoup, Comment

from custom_logger import log
from constants import GEMINI_PROMPT_TEMPLATE, DEFAULT_MODEL

# IMPORT ĐÚNG CỦA VERTEX AI
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig, HarmCategory, HarmBlockThreshold, Part

# from vertexai.generative_models import Type, Schema

load_dotenv()

# Khởi tạo Vertex AI (Sử dụng Service Account từ biến môi trường GOOGLE_APPLICATION_CREDENTIALS)
vertexai.init(
    project=os.getenv("GCP_PROJECT_ID"), 
    location=os.getenv("GCP_LOCATION", "us-central1")
)

# ==========================================
# 1. ĐỊNH NGHĨA LƯỚI LỌC PYDANTIC (SCHEMA)
# ==========================================
class AdCreativeData(BaseModel):
    video_url: Optional[str] = Field(description="Trích xuất link Media từ HTML. Ưu tiên 1: Link file video (thường có đuôi .mp4 nằm trong src=). Ưu tiên 2: Nếu không có video, lấy link hình ảnh. Nếu không có cả hai, bắt buộc để null.")
    ad_id: Optional[str] = Field(description="Trích xuất từ tham số 'id=' trong link")
    original_post_link: Optional[str] = Field(description="Đường link gốc của bài post")
    link_youtube: Optional[str] = Field(description="Link youtube nếu có")
    network: Optional[str] = Field(description="Nền tảng quảng cáo")
    language: Optional[str] = Field(description="Ngôn ngữ")
    region: Optional[str] = Field(description="Quốc gia hoặc Khu vực")
    duration: Optional[str] = Field(description="Thời gian chạy ads")
    start_date: Optional[str] = Field(description="Ngày bắt đầu")
    end_date: Optional[str] = Field(description="Ngày kết thúc")
    impression: Optional[str] = Field(description="Số lượt hiển thị (Impression)")
    
    top_1_percent_creative: bool = Field(description="Điền true nếu là top 1% creative")
    top_10_percent_creative: bool = Field(description="Điền true nếu là top 10% creative")
    
    headline: Optional[str] = Field(description="Tiêu đề của quảng cáo")
    headline_language: Optional[str] = Field(description="Ngôn ngữ của headline vừa lấy được(VD: en, vi, zh...)")
    headline_translated: Optional[str] = Field(description="Dịch sang TIẾNG VIỆT phần headline vừa lấy được")
    
    description: Optional[str] = Field(description="Mô tả nội dung của quảng cáo")
    description_language: Optional[str] = Field(description="Ngôn ngữ của description vừa lấy được (VD: en, vi...)")
    description_translated: Optional[str] = Field(description="Dịch sang TIẾNG VIỆT phần description vừa lấy được")

    transcript: Optional[str] = Field(description="Nội dung lời thoại video. Nếu không có thì để null.")
    transcript_language: Optional[str] = Field(description="Ngôn ngữ của transcript vừa lấy được. Nếu không có thì để null.")
    transcript_translated: Optional[str] = Field(description="Dịch sang TIẾNG VIỆT phần transcript vừa lấy được")

class TranscriptTranslationData(BaseModel):
    transcript: Optional[str] = Field(description="Nội dung lời thoại gốc nghe được. Nếu không có tiếng người, để null.")
    transcript_language: Optional[str] = Field(description="Ngôn ngữ của transcript (VD: en, vi...).")
    transcript_translated: Optional[str] = Field(description="Dịch sang TIẾNG VIỆT phần transcript vừa lấy được")

def clean_pydantic_schema_for_vertex(schema: dict) -> dict:
    """Loại bỏ kiểu 'null' trong anyOf do Vertex AI SDK chưa hỗ trợ."""
    cleaned = schema.copy()
    if "properties" in cleaned:
        for key, prop in cleaned["properties"].items():
            if "anyOf" in prop:
                # Tìm type thực sự (VD: 'string', 'boolean') và loại bỏ 'null'
                valid_types = [t["type"] for t in prop["anyOf"] if t.get("type") != "null"]
                if valid_types:
                    prop["type"] = valid_types[0]
                
                # Bật cờ nullable tiêu chuẩn của OpenAPI
                prop["nullable"] = True 
                del prop["anyOf"]
    return cleaned


def preprocess_html_for_llm(raw_html: str) -> str:
    if not raw_html or not isinstance(raw_html, str):
        return ""
        
    try:
        soup = BeautifulSoup(raw_html, 'lxml')

        unwanted_tags = [
            'script', 'style', 'noscript', 'meta', 'link', 'iframe', 
            'svg', 'path', 'g', 'polygon', 'rect', 'circle', 'line', 'polyline'
        ]
        for tag in soup.find_all(unwanted_tags):
            tag.decompose()

        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        allowed_attributes = {'href', 'src', 'poster', 'type', 'title', 'alt'}
        
        for tag in soup.find_all(True):
            tag.attrs = {k: v for k, v in tag.attrs.items() if k.lower() in allowed_attributes}

        cleaned_html = str(soup)
        cleaned_html = " ".join(cleaned_html.split())
        
        return cleaned_html

    except Exception as e:
        log.error(f"       -> [LỖI TIỀN XỬ LÝ HTML] {e}. Trả về HTML gốc để dự phòng.")
        return " ".join(raw_html.split())

# ==========================================
# 2. API HỖ TRỢ & DATABASE CONNECTION
# ==========================================
def process_media_for_transcript(video_url: str) -> dict:
    temp_id = str(uuid.uuid4())
    mp4_path = f"temp_{temp_id}.mp4"
    mp3_path = f"temp_{temp_id}.mp3"
    
    result = {
        "transcript": None,
        "transcript_language": None, 
        "transcript_translated": None
    }

    try:
        log.info(f"       -> [MEDIA] Đang tải file video: {video_url[:50]}...")
        r = requests.get(video_url, stream=True, timeout=120)
        r.raise_for_status()
        with open(mp4_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info(f"       -> [FFMPEG] Đang tách âm thanh sang định dạng MP3...")
        try:
            cmd = ['ffmpeg', '-y', '-i', mp4_path, '-vn', '-ar', '44100', '-ac', '2', '-b:a', '128k', mp3_path]
            process = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
        except subprocess.TimeoutExpired:
            log.error("       -> [CẢNH BÁO] Tiến trình FFMPEG bị treo quá 3 phút. Đã cưỡng chế ngắt.")
            return result

        if process.returncode != 0 or not os.path.exists(mp3_path) or os.path.getsize(mp3_path) == 0:
            log.warning("       -> [CẢNH BÁO] Video không có âm thanh hoặc lỗi convert FFMPEG. Bỏ qua dịch thuật.")
            result["transcript"] = "Video không có transcript"
            return result

        log.info(f"       -> [VERTEX AI] Đang đẩy MP3 lên AI để bóc tách lời thoại...")
        
        # Gọi trực tiếp qua bytes (không còn upload file stateful)
        with open(mp3_path, "rb") as f:
            audio_data = f.read()
        
        audio_part = Part.from_data(data=audio_data, mime_type="audio/mp3")
        model = GenerativeModel("gemini-2.5-flash-lite")
        prompt = "Dưới đây là một đoạn âm thanh. Hãy nghe, chép lại lời thoại gốc (transcript), xác định NGÔN NGỮ GỐC của nó và DỊCH toàn bộ nội dung sang TIẾNG VIỆT. Nếu chỉ có tiếng nhạc hoặc tạp âm mà không có lời thoại con người, hãy trả về toàn bộ là null. Nếu có transcript thì các trường transcript_language và transcript_translated KHÔNG ĐƯỢC PHÉP đặt là null"
        
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = model.generate_content(
                    [prompt, audio_part], 
                    safety_settings=safety_settings,
                    generation_config=GenerationConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                        response_schema=clean_pydantic_schema_for_vertex(TranscriptTranslationData.model_json_schema()), 
                    )
                )
                break  # Thoát vòng lặp nếu thành công
            except Exception as e:
                if "429" in str(e) or "Resource exhausted" in str(e):
                    if attempt < max_retries - 1:
                        wait_time = 5 * (attempt + 1)
                        log.warning(f"       -> [VERTEX AI AUDIO] Bị Rate Limit (429). Đang chờ {wait_time}s để thử lại (Lần {attempt + 1}/{max_retries})...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Lỗi 429 Audio: Đã thử lại {max_retries} lần vẫn thất bại.")
                else:
                    raise e

        parsed_data = TranscriptTranslationData.model_validate_json(response.text).model_dump()
        if not parsed_data.get("transcript") or str(parsed_data.get("transcript")).lower() == 'null':
            result["transcript"] = "Video không có transcript"
            result["transcript_language"] = None
            result["transcript_translated"] = None
        else:
            result.update(parsed_data)
        log.info("       -> [THÀNH CÔNG] Đã hoàn tất lấy Audio Transcript.")

    except Exception as e:
        log.error(f"       -> [LỖI PIPELINE MEDIA] {e}")
        result["transcript"] = "Video không có transcript"
    finally:
        # Dọn dẹp ổ cứng cục bộ triệt để (Đã xóa tàn dư genai.delete_file)
        if os.path.exists(mp4_path): os.remove(mp4_path)
        if os.path.exists(mp3_path): os.remove(mp3_path)

    return result

def get_db_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "")
        )
    except Exception as e:
        log.warning(f"Không thể kết nối Database để check Cache Toàn cục: {e}")
        return None

def parse_html_with_gemini(html: str, model_name: str) -> dict:
    cleaned_html = preprocess_html_for_llm(html)
    prompt = GEMINI_PROMPT_TEMPLATE.format(html=cleaned_html)
    
    model = GenerativeModel(model_name)
    
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = model.generate_content(
                prompt,
                safety_settings=safety_settings,
                generation_config=GenerationConfig(
                    temperature=0.1, 
                    response_mime_type="application/json",
                    response_schema=clean_pydantic_schema_for_vertex(AdCreativeData.model_json_schema()), 
                )
            )
            break  # Nếu thành công, thoát khỏi vòng lặp ngay
        except Exception as e:
            if "429" in str(e) or "Resource exhausted" in str(e):
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1) # Lần 1: 5s, Lần 2: 10s...
                    log.warning(f"       -> [VERTEX AI HTML] Bị Rate Limit (429). Đang chờ {wait_time}s để thử lại (Lần {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    raise ValueError(f"Lỗi 429: Đã thử lại {max_retries} lần nhưng hệ thống vẫn từ chối.")
            else:
                # Nếu là lỗi khác không phải 429 thì raise luôn để check
                raise ValueError(f"Lỗi hệ thống khi parse: {e}")

    try:
        raw_data = json.loads(response.text)
        for key in AdCreativeData.model_fields.keys():
            if key not in raw_data:
                if "percent_creative" in key: raw_data[key] = False
                else: raw_data[key] = None
        return raw_data
    except json.JSONDecodeError as e:
        raise ValueError(f"Dữ liệu Gemini trả về không phải JSON hợp lệ: {e}")
    except Exception as e:
        raise ValueError(f"Lỗi hệ thống khi parse: {e}")

# ==========================================
# 3. LUỒNG CHẠY BÓC TÁCH BUNDLE (MAIN)
# ==========================================
def process_bundle(input_filepath: str, model_name: str, no_transcript: bool = False):
    # Đã xóa dòng genai.configure() dư thừa
    with open(input_filepath, 'r', encoding='utf-8') as f:
        bundle = json.load(f)
    
    run_id = bundle.get("run_id", "unknown_run")
    total_apps_in_bundle = bundle.get("total_apps", 0)
    
    final_output = {
        "run_id": run_id,
        "parsed_at": datetime.now().isoformat(),
        "total_apps": total_apps_in_bundle,
        "successful_apps": 0,
        "apps": []
    }

    log.info(f"Bắt đầu Parse Bundle: {run_id} | Tổng số Apps: {total_apps_in_bundle}")
    
    conn = get_db_connection()
    if conn: log.info("Đã kết nối DB: Sẵn sàng Check Cache Lớp 2 (Toàn cục).")
    local_transcript_cache = {} 
    
    successful_apps_count = 0

    try:
        for app in bundle.get("apps", []):
            app_id = app.get("app_id")
            log.info(f"-> Đang xử lý App: {app_id}")
            
            parsed_app = {
                "app_id": app_id,
                "filters_applied": app.get("filters_applied", []),
                "scrape_statistics": app.get("scrape_statistics", {}), 
                "parse_statistics": {}, 
                "ads": []
            }
            
            total_received = len(app.get("ads", []))
            success_count = 0
            fail_count = 0
            
            for ad in app.get("ads", []):
                log.info(f"   + Bóc tách Ad Index {ad.get('ad_index')} (Trang {ad.get('page_number')})... ")
                raw_html = ad.get("raw_html", "")
                
                ad_result = {
                    "ad_index": ad.get("ad_index"),
                    "page_number": ad.get("page_number"),
                    "captured_at": ad.get("captured_at"),
                    "raw_html_length": len(raw_html),
                    "gemini_data": None,
                    "error": None
                }

                if not raw_html:
                    ad_result["error"] = "HTML rỗng"
                    fail_count += 1
                    log.warning("     [BỎ QUA] HTML rỗng.")
                else:
                    try:
                        gemini_html_data = parse_html_with_gemini(raw_html, model_name)
                        clean_video_url = gemini_html_data.get("video_url")
                        if isinstance(clean_video_url, str):
                            clean_video_url = clean_video_url.strip()
                        else:
                            clean_video_url = None

                        transcript_data = None
                        is_video_format = clean_video_url and ".mp4" in clean_video_url.lower()

                        if is_video_format:
                            if clean_video_url in local_transcript_cache and local_transcript_cache[clean_video_url].get("transcript") is not None:
                                log.info(f"       -> [CACHE LỚP 1] Tái sử dụng transcript (RAM) cho video: {clean_video_url[:30]}...")
                                transcript_data = local_transcript_cache[clean_video_url]
                            elif conn:
                                try:
                                    with conn.cursor() as cursor:
                                        cursor.execute("""
                                            SELECT transcript, video_language, transcript_translated
                                            FROM videos
                                            WHERE trim(video_url) = %s AND transcript IS NOT NULL
                                            LIMIT 1;
                                        """, (clean_video_url,))
                                        result = cursor.fetchone()
                                        
                                        if result:
                                            log.info(f"       -> [CACHE LỚP 2] Tái sử dụng dữ liệu đã có trong DB.")
                                            transcript_data = {
                                                "transcript": result[0],
                                                "transcript_language": result[1],
                                                "transcript_translated": result[2]
                                            }
                                            local_transcript_cache[clean_video_url] = transcript_data
                                except Exception as e:
                                    log.error(f"       -> [LỖI DB CACHE] {e}")
                                    conn.rollback()

                            if transcript_data:
                                gemini_html_data["transcript"] = transcript_data.get("transcript")
                                gemini_html_data["transcript_language"] = transcript_data.get("transcript_language")
                                gemini_html_data["transcript_translated"] = transcript_data.get("transcript_translated")
                            else:
                                if no_transcript:
                                    log.info("       -> [SKIP AUDIO] Cache Miss & Cờ no_transcript=True. Ép null, bỏ qua FFMPEG và Gemini.")
                                    new_transcript_data = {
                                        "transcript": None,
                                        "transcript_language": None,
                                        "transcript_translated": None
                                    }
                                else:
                                    log.info("       -> [CACHE MISS] Gọi tiến trình tải và tách lời thoại...")
                                    new_transcript_data = process_media_for_transcript(clean_video_url)
                                
                                gemini_html_data["transcript"] = new_transcript_data.get("transcript")
                                gemini_html_data["transcript_language"] = new_transcript_data.get("transcript_language")
                                gemini_html_data["transcript_translated"] = new_transcript_data.get("transcript_translated")
                                
                                local_transcript_cache[clean_video_url] = new_transcript_data

                        else:
                            if clean_video_url:
                                log.info("       -> [BỎ QUA] Đây là link hình ảnh tĩnh, không cần dịch Audio.")
                            gemini_html_data["transcript"] = None
                            gemini_html_data["transcript_language"] = None
                            gemini_html_data["transcript_translated"] = None

                        ad_result["gemini_data"] = gemini_html_data
                        success_count += 1
                        log.info("     [THÀNH CÔNG]")
                    except Exception as e:
                        ad_result["error"] = str(e)
                        fail_count += 1
                        log.error(f"     [LỖI] {e}")
                    
                    time.sleep(1.5)
                
                parsed_app["ads"].append(ad_result)
                
            parsed_app["parse_statistics"] = {
                "total_ads_received": total_received,
                "successfully_parsed_ads": success_count,
                "failed_to_parse": fail_count,
                "parse_success_rate": f"{success_count}/{total_received}" if total_received > 0 else "0/0"
            }
            
            if success_count > 0:
                successful_apps_count += 1
                
            final_output["apps"].append(parsed_app)

        final_output["successful_apps"] = successful_apps_count
        os.makedirs("crawl_json", exist_ok=True)
        output_filename = os.path.join("crawl_json", f"final_result_{run_id}.json")
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, ensure_ascii=False, indent=2)
            
        log.info(f"Hoàn thành! Đã lưu kết quả tại: {output_filename}")

        return output_filename

    finally:
        if conn:
            conn.close()
            log.info("Đã đóng kết nối DB (Cache).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Raw Bundle using Gemini Structured Outputs (Vertex AI)")
    parser.add_argument("input_file", type=str, help="Đường dẫn đến file raw_bundle_...json")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--no-transcript", action="store_true", help="Bỏ qua lấy transcript qua Gemini nếu Cache Miss")
    args = parser.parse_args()

    # Kiểm tra biến môi trường của Google Cloud
    if not os.getenv("GCP_PROJECT_ID") or not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        log.error("Thiếu GCP_PROJECT_ID hoặc GOOGLE_APPLICATION_CREDENTIALS trong environment")
        raise EnvironmentError("Yêu cầu thiết lập credentials của Google Cloud trước khi chạy.")

    # Đã loại bỏ tham số api_key
    process_bundle(args.input_file, args.model, args.no_transcript)