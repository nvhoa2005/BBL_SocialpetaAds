#!/usr/bin/env python3
import argparse
import json
import os
import time
import requests
from datetime import datetime
from typing import Optional

import google.generativeai as genai
from google.generativeai.types import GenerationConfig
from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv

from custom_logger import log
from constants import GEMINI_PROMPT_TEMPLATE, DEFAULT_MODEL

load_dotenv()

# ==========================================
# 1. ĐỊNH NGHĨA LƯỚI LỌC PYDANTIC (SCHEMA)
# ==========================================
class AdCreativeData(BaseModel):
    ad_id: Optional[str] = Field(description="Trích xuất từ tham số 'id=' trong link")
    original_post_link: Optional[str] = Field(description="Đường link gốc của bài post")
    link_youtube: Optional[str] = Field(description="Link youtube nếu có")
    network: Optional[str] = Field(description="Nền tảng quảng cáo")
    language: Optional[str] = Field(description="Ngôn ngữ")
    region: Optional[str] = Field(description="Quốc gia hoặc Khu vực")
    duration: Optional[str] = Field(description="Thời lượng video")
    start_date: Optional[str] = Field(description="Ngày bắt đầu")
    end_date: Optional[str] = Field(description="Ngày kết thúc")
    impression: Optional[str] = Field(description="Số lượt hiển thị (Impression)")
    
    top_1_percent_creative: bool = Field(description="Điền true nếu là top 1% creative")
    top_10_percent_creative: bool = Field(description="Điền true nếu là top 10% creative")
    
    headline: Optional[str] = Field(description="Tiêu đề của quảng cáo")
    headline_language: Optional[str] = Field(description="Ngôn ngữ của headline vừa lấy được(VD: en, vi, zh...)")
    headline_translated: Optional[str] = Field(description="Dịch sang Tiếng Việt phần headline vừa lấy được")
    
    description: Optional[str] = Field(description="Mô tả nội dung của quảng cáo")
    description_language: Optional[str] = Field(description="Ngôn ngữ của description vừa lấy được (VD: en, vi...)")
    description_translated: Optional[str] = Field(description="Dịch sang Tiếng Việt phần description vừa lấy được")

    transcript: Optional[str] = Field(description="Nội dung lời thoại video. Nếu không có thì để null.")
    transcript_language: Optional[str] = Field(description="Ngôn ngữ của transcript vừa lấy được. Nếu không có thì để null.")
    transcript_translated: Optional[str] = Field(description="Dịch sang Tiếng Việt phần transcript vừa lấy được")

# Schema phụ để parse kết quả dịch từ Gemini Flash Lite
class TranscriptTranslationData(BaseModel):
    transcript_language: Optional[str] = Field(description="Ngôn ngữ của transcript (VD: en, vi...).")
    transcript_translated: Optional[str] = Field(description="Dịch sang Tiếng Việt phần transcript vừa lấy được")

# ==========================================
# 2. XỬ LÝ API YOUTUBE TRANSCRIPT & DỊCH
# ==========================================
def get_youtube_transcript_from_api(url: str) -> str:
    """Gọi API nội bộ để lấy transcript text từ Youtube URL"""
    api_url = "https://script.stemlabs.site/transcript-url"
    headers = {
        "x-api-key": "1df1c391-ec19-4e5e-980a-02c0ac5de7af",
        "Content-Type": "application/json"
    }
    payload = {
        "video_url": url
    }
    
    try:
        log.info(f"       -> Đang gọi API lấy Transcript cho: {url}")
        response = requests.post(api_url, json=payload, headers=headers, timeout=60)
        
        if response.status_code == 200:
            # Vì API trả về thẳng chuỗi string
            transcript_text = response.text.strip()
            
            # Xử lý trường hợp API trả về rỗng hoặc lỗi string nhưng status 200
            if not transcript_text or transcript_text.lower() in ['null', 'none', '{}', 'error']:
                return None
            return transcript_text
        else:
            log.error(f"       -> [LỖI API] Status {response.status_code}: {response.text}")
            return None
    except Exception as e:
        log.error(f"       -> [LỖI NETWORK] Không thể gọi API transcript: {e}")
        return None

def translate_transcript_with_gemini(transcript: str) -> dict:
    """Sử dụng Gemini 2.5 Flash Lite để xác định ngôn ngữ và dịch transcript"""
    log.info(f"       -> Đang dịch Transcript ({len(transcript)} ký tự) bằng gemini-2.5-flash-lite...")
    
    # Ép dùng flash-lite cho rẻ và nhanh
    model = genai.GenerativeModel("gemini-2.5-flash-lite") 
    prompt = f"Dưới đây là một đoạn transcript. Hãy xác định ngôn ngữ gốc của nó và dịch toàn bộ sang tiếng Việt.\n\nTranscript: {transcript}"
    
    try:
        response = model.generate_content(
            prompt,
            generation_config=GenerationConfig(
                temperature=0.1,
                response_mime_type="application/json",
                response_schema=TranscriptTranslationData,
            )
        )
        parsed_data = TranscriptTranslationData.model_validate_json(response.text).model_dump()
        return parsed_data
    except Exception as e:
        log.error(f"       -> [LỖI GEMINI] Khi dịch transcript: {e}")
        return {"transcript_language": None, "transcript_translated": None}

# ==========================================
# 3. HÀM GỌI GEMINI CHO HTML
# ==========================================
def parse_html_with_gemini(html: str, model_name: str) -> dict:
    prompt = GEMINI_PROMPT_TEMPLATE.format(html=html)
    model = genai.GenerativeModel(model_name)
    response = model.generate_content(
        prompt,
        generation_config=GenerationConfig(
            temperature=0.1, 
            response_mime_type="application/json",
            response_schema=AdCreativeData, 
        )
    )
    try:
        import json
        raw_data = json.loads(response.text)
        
        for key in AdCreativeData.model_fields.keys():
            if key not in raw_data:
                if "percent_creative" in key:
                    raw_data[key] = False
                else:
                    raw_data[key] = None
                    
        return raw_data
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Dữ liệu Gemini trả về không phải JSON hợp lệ: {e}")
    except Exception as e:
        raise ValueError(f"Lỗi hệ thống khi parse: {e}")

# ==========================================
# 4. LUỒNG CHẠY BÓC TÁCH BUNDLE (MAIN)
# ==========================================
def process_bundle(input_filepath: str, api_key: str, model_name: str):
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
    successful_apps_count = 0

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
                    # BƯỚC 1: PARSE HTML
                    gemini_html_data = parse_html_with_gemini(raw_html, model_name)
                    
                    # BƯỚC 2: XỬ LÝ YOUTUBE AUDIO (NẾU CÓ) QUA API NGOÀI
                    link_yt = gemini_html_data.get("link_youtube")
                    if link_yt and ("youtube.com" in link_yt or "youtu.be" in link_yt):
                        # Lấy Transcript gốc
                        transcript_text = get_youtube_transcript_from_api(link_yt)
                        
                        if transcript_text:
                            # Nếu lấy được nội dung, dùng Gemini Flash Lite để dịch
                            translation_data = translate_transcript_with_gemini(transcript_text)
                            
                            # Cập nhật kết quả vào cục data lớn
                            gemini_html_data["transcript"] = transcript_text
                            gemini_html_data["transcript_language"] = translation_data.get("transcript_language")
                            gemini_html_data["transcript_translated"] = translation_data.get("transcript_translated")
                            log.info("       -> Hoàn thành xử lý Transcript.")
                        else:
                            # Set None nếu không có
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Raw Bundle using Gemini Structured Outputs")
    parser.add_argument("input_file", type=str, help="Đường dẫn đến file raw_bundle_...json")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    args = parser.parse_args()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log.error("Thiếu GEMINI_API_KEY trong environment")
        raise EnvironmentError("Thiếu GEMINI_API_KEY trong environment")

    process_bundle(args.input_file, api_key, args.model)