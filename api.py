import uuid
import os
import json
import time
import psycopg2
from db_import import DB_CONFIG
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import List, Optional

from crawler import run as run_crawler
from constants import TIME_FILTERS, SORT_FILTERS, DROPDOWN_SORTS
from custom_logger import log

from parse_with_gemini import process_bundle
from export_excel import json_to_excel, export_page_id_excel
from constants import DEFAULT_MODEL

app = FastAPI(title="SocialPeta Crawler API")

TASKS_DB = {}

class CrawlRequest(BaseModel):
    app_id: str = Field(..., description="Danh sách App ID, cách nhau bởi dấu xuống dòng (\\n)")
    networks: List[str] = Field(default=["youtube"], description="Mảng nền tảng muốn cào. VD: ['tiktok', 'youtube']")
    time_val: str = Field(default="90 Days")
    sort_val: str = Field(default="Impression")
    max_ads: int = Field(default=100)
    start_page: int = Field(default=1)
    auto_resume_crawl_if_fail: bool = Field(default=True)
    time_to_resume: int = Field(default=60)
    crawl_page_id: bool = Field(default=False, description="Nếu True: Chỉ cào Page ID trên Facebook, không qua Gemini, xuất Excel theo cột.")

# Request Model để Frontend gửi quyết định
class ResolveKickoutRequest(BaseModel):
    action: str = Field(..., description="Nhận giá trị: 'resume' hoặc 'stop'")
    delay: int = Field(default=60, description="Thời gian chờ (giây) trước khi resume")

def sync_task_status(task_id: str, new_status: str, message: str):
    """Cập nhật trạng thái đồng thời trên RAM và PostgreSQL"""
    if task_id in TASKS_DB:
        TASKS_DB[task_id]["status"] = new_status
        TASKS_DB[task_id]["current_action"] = message
        log.info(f"[Task {task_id} -> {new_status.upper()}]: {message}")

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE crawl_tasks 
            SET status = %s 
            WHERE upstream_task_id = %s
        """, (new_status.upper(), task_id))
        conn.commit()
    except Exception as e:
        log.error(f"Lỗi đồng bộ DB Status cho task {task_id}: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def background_crawl_task(task_id: str, req: CrawlRequest):
    def update_status_callback(message: str):
        if task_id in TASKS_DB:
            TASKS_DB[task_id]["current_action"] = message
            log.info(f"[Task {task_id} Status]: {message}")

    # Hàm Callback "Cầu nối" để Crawler dừng lại và chờ Frontend
    def wait_for_user_callback(app_id, current_ads, max_ads, timeout_seconds=1800):
        msg = f"Tài khoản bị văng. Đã cào được {current_ads}/{max_ads} ads của app id {app_id}. Bạn muốn resume không?"
        sync_task_status(task_id, "WAITING_FOR_USER", msg)
        TASKS_DB[task_id]["user_action"] = None
        
        start_time = time.time()
        # Vòng lặp chờ tối đa 30 phút (1800s)
        while time.time() - start_time < timeout_seconds:
            action = TASKS_DB[task_id].get("user_action")
            if action:
                # DÙNG SYNC_TASK_STATUS VÀ TRẢ VỀ ĐÚNG TRẠNG THÁI 'SCRAPING'
                sync_task_status(task_id, "SCRAPING", f"Nhận lệnh '{action}' từ người dùng. Đang tiếp tục xử lý...")
                return action, TASKS_DB[task_id].get("resume_delay", 60)
            time.sleep(1)
        
        # HẾT GIỜ: Tự động chốt kết quả (Stop)
        sync_task_status(task_id, "SCRAPING", "Quá 30 phút không phản hồi. Tự động chốt kết quả hiện tại...")
        return "stop", 0

    TASKS_DB[task_id]["user_cancelled"] = False
    def check_cancel_callback():
        return TASKS_DB.get(task_id, {}).get("user_cancelled", False)
    
    try:
        log.info(f"Task {task_id} bắt đầu chạy ngầm...")
        
        # --- PHASE 1: SCRAPING (Chiếm dụng trình duyệt) ---
        sync_task_status(task_id, "SCRAPING", "Hệ thống đang khởi tạo trình duyệt cào dữ liệu...")
        
        raw_app_ids = req.app_id.strip().split('\n')
        app_ids = [aid.strip() for aid in raw_app_ids if aid.strip()]
        time_val = req.time_val if req.time_val in TIME_FILTERS else "90 Days"
        sort_val = req.sort_val if req.sort_val in SORT_FILTERS + DROPDOWN_SORTS else "Impression"
        
        tasks_list = [{
            "app_id": aid, "networks": req.networks, "time_val": time_val,
            "sort_val": sort_val, "max_ads": req.max_ads, "start_page": req.start_page,
            "crawl_page_id": req.crawl_page_id
        } for aid in app_ids]
            
        # Gọi crawler, lúc này run_crawler sẽ trả về dictionary
        crawl_result = run_crawler(
            api_tasks=tasks_list, 
            custom_run_id=task_id,
            auto_resume=req.auto_resume_crawl_if_fail,
            time_to_resume=req.time_to_resume,
            status_callback=update_status_callback,
            wait_for_user_callback=wait_for_user_callback,
            check_cancel_callback=check_cancel_callback
        )
        
        # Validate kết quả trả về từ Crawler
        if not crawl_result:
            raise Exception("Lỗi hệ thống: Crawler không trả về dữ liệu.")

        raw_file_path = crawl_result.get("raw_file_path")
        is_page_id_run = crawl_result.get("is_page_id_run")
        
        if not raw_file_path or not os.path.exists(raw_file_path):
            raise Exception("Không tìm thấy file raw data sau khi cào (Hoặc không có task nào hợp lệ).")

        # KIỂM TRA LỆNH HỦY TỪ USER
        is_cancelled = TASKS_DB.get(task_id, {}).get("user_cancelled", False)

        # --- CHUYỂN GIAO TRẠNG THÁI (MỞ KHÓA TÀI KHOẢN SOCIALPETA CHO HÀNG ĐỢI) ---
        if is_cancelled:
            sync_task_status(task_id, "PARSING_GEMINI", "Đã nhận lệnh hủy. Giải phóng trình duyệt và đang xử lý/đóng gói phần dữ liệu đã cào được...")
        else:
            sync_task_status(task_id, "PARSING_GEMINI", "Đã giải phóng trình duyệt. Đang đẩy data qua Gemini và xuất Excel...")

        # --- PHASE 2: PARSING & EXPORTING (Chạy song song, không cần trình duyệt) ---
        excel_path = None
        if is_page_id_run:
            excel_path = export_page_id_excel(raw_file_path)
        else:
            api_key = os.getenv("GEMINI_API_KEY")
            if not api_key:
                raise Exception("Thiếu GEMINI_API_KEY trong file .env")
                
            final_json_path = process_bundle(raw_file_path, api_key, DEFAULT_MODEL)
            if final_json_path:
                excel_path = json_to_excel(final_json_path)
            else:
                raise Exception("Lỗi bóc tách Gemini, không sinh được file final JSON.")

        # --- HOÀN TẤT & CHỐT STATUS ---
        TASKS_DB[task_id]["result_file"] = final_json_path if not is_page_id_run else raw_file_path
        if excel_path and os.path.exists(excel_path):
            TASKS_DB[task_id]["excel_file"] = excel_path
            TASKS_DB[task_id]["download_url"] = f"/api/v1/download/{task_id}"

        # Nếu user đã ấn Cancel trước đó, chốt hạ bằng status Cancelled để UI nhận diện
        final_is_cancelled = TASKS_DB.get(task_id, {}).get("user_cancelled", False)
        if final_is_cancelled:
            sync_task_status(task_id, "CANCELLED", "Đã hoàn thành đóng gói dữ liệu dở dang do người dùng yêu cầu hủy.")
        else:
            sync_task_status(task_id, "COMPLETED", "Đã hoàn thành toàn bộ Pipeline.")
        
    except Exception as e:
        sync_task_status(task_id, "FAILED", f"Lỗi hệ thống: {str(e)}")
        log.error(f"Task {task_id} bị lỗi: {e}")

# --- API ENDPOINTS ---
@app.post("/api/v1/crawl")
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    task_id = datetime.now().strftime("crawl_%Y%m%d_%H%M%S_") + str(uuid.uuid4())[:6]
    TASKS_DB[task_id] = {
        "status": "PENDING",
        "current_action": "Đang chờ điều phối luồng...",
        "created_at": datetime.now().isoformat(),
        "total_apps": len([aid for aid in request.app_id.split('\n') if aid.strip()])
    }
    background_tasks.add_task(background_crawl_task, task_id, request)
    return {"task_id": task_id, "status": "PENDING"}

@app.get("/api/v1/status/{task_id}")
async def get_status(task_id: str):
    if task_id not in TASKS_DB:
        raise HTTPException(status_code=404, detail="Không tìm thấy task_id.")
    task_info = TASKS_DB[task_id]
    if task_info["status"] in ["COMPLETED", "cancelled", "CANCELLED", "completed"]:
        result_file = task_info.get("result_file")
        if result_file and os.path.exists(result_file):
            with open(result_file, 'r', encoding='utf-8') as f:
                task_info["data"] = json.load(f)
    return task_info

@app.post("/api/v1/task/{task_id}/resolve-kickout")
async def resolve_kickout(task_id: str, request: ResolveKickoutRequest):
    if task_id not in TASKS_DB:
        log.info(f'Not found {task_id}')
        raise HTTPException(status_code=404, detail="Không tìm thấy task_id.")
    if TASKS_DB[task_id].get("status") != "WAITING_FOR_USER":
        raise HTTPException(status_code=400, detail="Task hiện tại không ở trạng thái chờ quyết định.")
    if request.action not in ["resume", "stop"]:
        raise HTTPException(status_code=400, detail="Action không hợp lệ.")
        
    TASKS_DB[task_id]["user_action"] = request.action
    TASKS_DB[task_id]["resume_delay"] = request.delay
    return {"status": "success", "message": f"Đã ghi nhận hành động: {request.action}"}

@app.get("/api/v1/download/{task_id}")
async def download_excel(task_id: str):
    excel_file = None
    
    if task_id in TASKS_DB:
        task_info = TASKS_DB[task_id]
        if task_info.get("status") not in ["completed", "cancelled", "COMPLETED", "CANCELLED"]:
            raise HTTPException(status_code=400, detail="Task đang chạy chưa hoàn thành, không thể tải file.")
        excel_file = task_info.get("excel_file")
        
    else:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            cursor.execute("SELECT status FROM crawl_tasks WHERE upstream_task_id = %s", (task_id,))
            result = cursor.fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail=f"Không tìm thấy task_id: {task_id} trong hệ thống.")
            
            db_status = str(result[0]).strip().upper() 
            
            if db_status not in ['COMPLETED', 'CANCELLED']:
                raise HTTPException(status_code=400, detail=f"Task hiện tại ở trạng thái {db_status}, chưa hoàn chỉnh dữ liệu Excel.")
            
            excel_file = os.path.join("crawl_results", f"excel_result_{task_id}.xlsx")
            
        except psycopg2.Error as e:
            log.error(f"Lỗi truy vấn database khi download file Excel: {e}")
            raise HTTPException(status_code=500, detail="Lỗi kết nối kiểm tra dữ liệu từ Database.")
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

    if not excel_file or not os.path.exists(excel_file):
        raise HTTPException(status_code=404, detail="Dữ liệu báo đã hoàn thành nhưng file Excel vật lý không tồn tại hoặc đã bị xóa.")

    filename = os.path.basename(excel_file)
    return FileResponse(
        path=excel_file,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/api/v1/task/{task_id}/cancel-info")
async def get_cancel_info(task_id: str):
    if task_id not in TASKS_DB:
        raise HTTPException(status_code=404, detail="Không tìm thấy task_id.")
    
    output_filename = os.path.join("crawl_json", f"raw_bundle_{task_id}.json")
    details = "Thông tin hiện tại chưa sẵn sàng."
    
    if os.path.exists(output_filename):
        try:
            with open(output_filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                apps = data.get("apps", [])
                if apps:
                    current_app = apps[-1]
                    crawled = len(current_app.get("ads", []))
                    total = current_app.get("scrape_statistics", {}).get("requested_max_ads", 0)
                    details = f"Đang cào App {current_app['app_id']}: {crawled}/{total} ads."
                    if len(apps) > 1:
                        details += f" (Đã hoàn thành {len(apps)-1} apps trước đó)."
        except Exception:
            pass

    return {
        "task_id": task_id,
        "message": "Bạn có chắc chắn muốn dừng không? Dữ liệu đã cào sẽ vẫn được xử lý.",
        "details": details
    }

@app.post("/api/v1/task/{task_id}/cancel")
async def cancel_task(task_id: str):
    if task_id not in TASKS_DB:
        raise HTTPException(status_code=404, detail="Không tìm thấy task_id.")
    
    if TASKS_DB[task_id].get("status") in ["completed", "cancelled", "failed", "COMPLETED", "CANCELLED", "FAILED"]:
        raise HTTPException(status_code=400, detail="Task đã kết thúc, không thể thực hiện lệnh hủy.")
    
    TASKS_DB[task_id]["user_cancelled"] = True
    TASKS_DB[task_id]["current_action"] = "Đang tiếp nhận lệnh HỦY từ người dùng. Đang đóng gói dữ liệu..."
    
    return {"status": "SUCCESS", "message": "Đã gửi yêu cầu dừng. Vui lòng đợi trong giây lát để hệ thống xử lý dữ liệu hiện có."}

@app.get("/api/v1/task/{task_id}/page-id-info")
async def get_page_id_info(task_id: str):
    # 1. Kiểm tra trạng thái Task (ưu tiên RAM, fallback xuống Database nếu server vừa restart)
    task_status = None
    
    if task_id in TASKS_DB:
        task_status = TASKS_DB[task_id].get("status")
    else:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT status FROM crawl_tasks WHERE upstream_task_id = %s", (task_id,))
            result = cursor.fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail="Không tìm thấy task_id trong RAM lẫn Database.")
            
            db_status = str(result[0]).strip().upper()
            task_status = db_status
            
        except psycopg2.Error as e:
            log.error(f"Lỗi truy vấn DB khi lấy info Page ID: {e}")
            raise HTTPException(status_code=500, detail="Lỗi kết nối kiểm tra dữ liệu từ Database.")
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

    # 2. Ràng buộc: Chỉ trả về data khi task đã chạy xong hoàn toàn
    if task_status not in ["completed", "cancelled", "COMPLETED", "CANCELLED"]:
        raise HTTPException(
            status_code=400, 
            detail=f"Task hiện tại đang ở trạng thái '{task_status}', chưa hoàn thành."
        )

    # 3. Đọc dữ liệu thô từ file raw_bundle
    raw_bundle_path = os.path.join("crawl_json", f"raw_bundle_{task_id}.json")
    if not os.path.exists(raw_bundle_path):
        raise HTTPException(
            status_code=404, 
            detail="Không tìm thấy file checkpoint dữ liệu của task này trên ổ cứng."
        )

    try:
        with open(raw_bundle_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Lỗi hệ thống khi đọc file dữ liệu: {str(e)}"
        )

    # 4. Xử lý và mapping dữ liệu theo chuẩn JSON chuyên nghiệp
    apps_data = []
    all_page_ids_list = []

    for app in data.get("apps", []):
        app_id = app.get("app_id")
        
        raw_page_ids = app.get("page_ids", [])
        extracted_ids = [item["page_id"] for item in raw_page_ids if "page_id" in item]

        if not extracted_ids:
            page_ids_value = None
        else:
            page_ids_value = extracted_ids
            all_page_ids_list.extend(extracted_ids)

        apps_data.append({
            "app_id": app_id,
            "page_ids": page_ids_value
        })

    # Lọc trùng lặp mảng tổng (bảo toàn thứ tự cào được)
    all_page_ids_unique = list(dict.fromkeys(all_page_ids_list))

    return {
        "apps_data": apps_data,
        "all_page_ids": all_page_ids_unique
    }

# uvicorn api:app --reload
# uvicorn api:app --host 192.168.1.68 --port 8000 
# nohup uvicorn api:app --host 192.168.1.68 --port 8000 > api.log 2>&1 &