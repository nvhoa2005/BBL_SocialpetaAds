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

import google.generativeai as genai
from google.generativeai.types import GenerationConfig, HarmCategory, HarmBlockThreshold
from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv

from custom_logger import log
from constants import GEMINI_PROMPT_TEMPLATE, DEFAULT_MODEL

load_dotenv()

# ==========================================
# 1. ĐỊNH NGHĨA LƯỚI LỌC PYDANTIC (SCHEMA)
# ==========================================
class AdCreativeData(BaseModel):
    video_url: Optional[str] = Field(description="Trích xuất link Media từ HTML. Ưu tiên 1: Link file video (thường có đuôi .mp4 nằm trong thẻ <video> hoặc <source>). Ưu tiên 2: Nếu không có video, lấy link hình ảnh. Nếu không có cả hai, bắt buộc để null.")
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
# ==========================================
# 2. API HỖ TRỢ & DATABASE CONNECTION
# ==========================================
def process_media_for_transcript(video_url: str) -> dict:
    """Tải MP4 -> Convert MP3 qua FFMPEG -> Đẩy lên Gemini Audio -> Xóa rác"""
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
        # 1. Download Video
        r = requests.get(video_url, stream=True, timeout=120)
        r.raise_for_status()
        with open(mp4_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info(f"       -> [FFMPEG] Đang tách âm thanh sang định dạng MP3...")
        # 2. Chạy FFMPEG (Cờ -vn để loại bỏ hình ảnh, chỉ lấy audio)
        try:
            cmd = ['ffmpeg', '-y', '-i', mp4_path, '-vn', '-ar', '44100', '-ac', '2', '-b:a', '128k', mp3_path]
            # Ép timeout 3 phút tối đa cho một lần convert
            process = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
        except subprocess.TimeoutExpired:
            log.error("       -> [CẢNH BÁO] Tiến trình FFMPEG bị treo quá 3 phút. Đã cưỡng chế ngắt.")
            return result

        if process.returncode != 0 or not os.path.exists(mp3_path) or os.path.getsize(mp3_path) == 0:
            log.warning("       -> [CẢNH BÁO] Video không có âm thanh hoặc lỗi convert FFMPEG. Bỏ qua dịch thuật.")
            result["transcript"] = "Video không có transcript"
            return result

        log.info(f"       -> [GEMINI AUDIO] Đang đẩy MP3 lên AI để bóc tách lời thoại...")
        audio_file = None
        try:
            # 3. Đẩy lên Gemini Storage
            audio_file = genai.upload_file(path=mp3_path)

            # 4. Gọi Model
            model = genai.GenerativeModel("gemini-2.5-flash-lite")
            prompt = "Dưới đây là một đoạn âm thanh. Hãy nghe, chép lại lời thoại gốc (transcript), xác định NGÔN NGỮ GỐC của nó và DỊCH toàn bộ nội dung sang TIẾNG VIỆT. Nếu chỉ có tiếng nhạc hoặc tạp âm mà không có lời thoại con người, hãy trả về toàn bộ là null. Nếu có transcript thì các trường transcript_language và transcript_translated KHÔNG ĐƯỢC PHÉP đặt là null"
            
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }

            response = model.generate_content(
                [prompt, audio_file],
                safety_settings=safety_settings,
                generation_config=GenerationConfig(
                    temperature=0.1,
                    response_mime_type="application/json",
                    response_schema=TranscriptTranslationData,
                )
            )
            
            # 5. Map dữ liệu
            parsed_data = TranscriptTranslationData.model_validate_json(response.text).model_dump()
            if not parsed_data.get("transcript") or str(parsed_data.get("transcript")).lower() == 'null':
                result["transcript"] = "Video không có transcript"
                result["transcript_language"] = None
                result["transcript_translated"] = None
            else:
                result.update(parsed_data)
            log.info("       -> [THÀNH CÔNG] Đã hoàn tất lấy Audio Transcript.")
        finally:
            # LUÔN LUÔN xóa file trên Cloud dù thành công hay lỗi
            if audio_file:
                try:
                    genai.delete_file(audio_file.name)
                except Exception as del_err:
                    log.error(f"       -> [LỖI DỌN RÁC CLOUD] Không thể xóa file {audio_file.name}: {del_err}")

    except Exception as e:
        log.error(f"       -> [LỖI PIPELINE MEDIA] {e}")
        result["transcript"] = "Video không có transcript"
    finally:
        # 6. Dọn dẹp ổ cứng cục bộ triệt để
        if os.path.exists(mp4_path): os.remove(mp4_path)
        if os.path.exists(mp3_path): os.remove(mp3_path)

    return result

def get_db_connection():
    """Tạo kết nối tới PostgreSQL để check cache toàn cục"""
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

# def get_youtube_transcript_from_api(url: str) -> str:
#     """Gọi API nội bộ để lấy transcript text từ Youtube URL"""
#     api_url = "https://script.stemlabs.site/transcript-url"
#     headers = {
#         "x-api-key": "1df1c391-ec19-4e5e-980a-02c0ac5de7af",
#         "Content-Type": "application/json"
#     }
#     payload = {"video_url": url}
    
#     try:
#         log.info(f"       -> Đang gọi API lấy Transcript cho: {url}")
#         response = requests.post(api_url, json=payload, headers=headers, timeout=60)
        
#         if response.status_code == 200:
#             transcript_text = response.text.strip()
#             if not transcript_text or transcript_text.lower() in ['null', 'none', '{}', 'error']:
#                 return None
#             return transcript_text
#         else:
#             log.error(f"       -> [LỖI API] Status {response.status_code}: {response.text}")
#             return None
#     except Exception as e:
#         log.error(f"       -> [LỖI NETWORK] Không thể gọi API transcript: {e}")
#         return None

# def translate_transcript_with_gemini(transcript: str) -> dict:
#     """Sử dụng Gemini 2.5 Flash Lite để xác định ngôn ngữ và dịch transcript"""
#     log.info(f"       -> Đang dịch Transcript ({len(transcript)} ký tự) bằng gemini-2.5-flash-lite...")
#     model = genai.GenerativeModel("gemini-2.5-flash-lite") 
#     prompt = f"Dưới đây là một đoạn transcript. Hãy xác định ngôn ngữ gốc của nó và dịch toàn bộ sang tiếng Việt.\n\nTranscript: {transcript}"
    
#     try:
#         response = model.generate_content(
#             prompt,
#             generation_config=GenerationConfig(
#                 temperature=0.1,
#                 response_mime_type="application/json",
#                 response_schema=TranscriptTranslationData,
#             )
#         )
#         parsed_data = TranscriptTranslationData.model_validate_json(response.text).model_dump()
#         return parsed_data
#     except Exception as e:
#         log.error(f"       -> [LỖI GEMINI] Khi dịch transcript: {e}")
#         return {"transcript_language": None, "transcript_translated": None}

def parse_html_with_gemini(html: str, model_name: str) -> dict:
    prompt = GEMINI_PROMPT_TEMPLATE.format(html=html)
    model = genai.GenerativeModel(model_name)
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }
    response = model.generate_content(
        prompt,
        safety_settings=safety_settings,
        generation_config=GenerationConfig(
            temperature=0.1, 
            response_mime_type="application/json",
            response_schema=AdCreativeData, 
        )
    )
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
def process_bundle(input_filepath: str, api_key: str, model_name: str, no_transcript: bool = False):
    genai.configure(api_key=api_key)
    
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
    
    # KHOI TAO CACHE 2 LOP
    conn = get_db_connection()
    if conn: log.info("Đã kết nối DB: Sẵn sàng Check Cache Lớp 2 (Toàn cục).")
    local_transcript_cache = {} # Bộ nhớ đệm Lớp 1 (Cục bộ)
    
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
                        # BƯỚC 1: PARSE HTML (Lấy ra video_url)
                        gemini_html_data = parse_html_with_gemini(raw_html, model_name)
                        
                        # BƯỚC 2: PHÂN LUỒNG MEDIA & CACHE (Theo video_url)
                        clean_video_url = gemini_html_data.get("video_url")
                        if isinstance(clean_video_url, str):
                            clean_video_url = clean_video_url.strip()
                        else:
                            clean_video_url = None

                        transcript_data = None

                        # Kiểm tra xem link có phải là dạng Video cần bóc audio không (.mp4)
                        is_video_format = clean_video_url and ".mp4" in clean_video_url.lower()

                        if is_video_format:
                            # 2.1 Check Cache Lớp 1 (RAM)
                            # FIX: Phải đảm bảo giá trị trong RAM KHÁC None thì mới tính là Hit
                            if clean_video_url in local_transcript_cache and local_transcript_cache[clean_video_url].get("transcript") is not None:
                                log.info(f"       -> [CACHE LỚP 1] Tái sử dụng transcript (RAM) cho video: {clean_video_url[:30]}...")
                                transcript_data = local_transcript_cache[clean_video_url]
                                
                            # 2.2 Check Cache Lớp 2 (Database Toàn cục)
                            elif conn:
                                try:
                                    with conn.cursor() as cursor:
                                        # FIX: Bổ sung "AND transcript IS NOT NULL"
                                        # - Bỏ qua các dòng bị ép null (no_transcript=True từ trước)
                                        # - Lấy thành công các dòng 'Video không có transcript' (vì nó là chuỗi, khác NULL)
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

                            # 2.3 Thực thi phân luồng
                            if transcript_data:
                                # Hit Cache hợp lệ (Dữ liệu chữ thật, hoặc chuỗi 'Video không có transcript')
                                gemini_html_data["transcript"] = transcript_data.get("transcript")
                                gemini_html_data["transcript_language"] = transcript_data.get("transcript_language")
                                gemini_html_data["transcript_translated"] = transcript_data.get("transcript_translated")
                            else:
                                # CAN THIỆP LOGIC Ở ĐÂY: Cache miss thì check cờ no_transcript
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
                                
                                # LƯU VÀO RAM CACHE BẤT KỂ KẾT QUẢ LÀ GÌ
                                local_transcript_cache[clean_video_url] = new_transcript_data

                        else:
                            # Nếu là Hình ảnh tĩnh hoặc null -> Cưỡng ép toàn bộ transcript về null
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

        # Cập nhật số lượng app thành công
        final_output["successful_apps"] = successful_apps_count
        os.makedirs("crawl_json", exist_ok=True)
        output_filename = os.path.join("crawl_json", f"final_result_{run_id}.json")
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, ensure_ascii=False, indent=2)
            
        log.info(f"Hoàn thành! Đã lưu kết quả tại: {output_filename}")

        return output_filename

    finally:
        # Đảm bảo tắt kết nối DB an toàn dù có lỗi xảy ra
        if conn:
            conn.close()
            log.info("Đã đóng kết nối DB (Cache).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Raw Bundle using Gemini Structured Outputs")
    parser.add_argument("input_file", type=str, help="Đường dẫn đến file raw_bundle_...json")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--no-transcript", action="store_true", help="Bỏ qua lấy transcript qua Gemini nếu Cache Miss")
    args = parser.parse_args()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log.error("Thiếu GEMINI_API_KEY trong environment")
        raise EnvironmentError("Thiếu GEMINI_API_KEY trong environment")

    process_bundle(args.input_file, api_key, args.model, args.no_transcript)