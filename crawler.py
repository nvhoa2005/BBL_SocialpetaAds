from playwright.sync_api import sync_playwright
import json
import os
import re
import random
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright_stealth import Stealth

from custom_logger import log
from constants import (
    TARGET_URL, DEFAULT_TIMEOUT, LONG_TIMEOUT, Selectors, 
    DROPDOWN_SORTS, TIME_FILTERS, SORT_FILTERS, 
    WAIT_FOR_USER_CALLBACK, FACEBOOK_NETWORK_NAME_PAGE_ID,
    VIEWPORT_WIDTH, VIEWPORT_HEIGHT, DEFAULT_MODEL, 
    OUTSIDE_NETWORKS, INSIDE_NETWORKS
)  
from human_behavior import (
    human_click, human_click_safe_zone, human_type, human_smooth_scroll, 
    human_delay, human_idle_mouse_move, show_mouse_cursor, human_aimless_highlight,
    human_wait_with_jitter, human_reading_trace, human_retreat_mouse,
    human_navigate_to_top, human_navigate_to_bottom,
    human_close_modal
)
from parse_with_gemini import process_bundle
from export_excel import json_to_excel, export_page_id_excel

load_dotenv()

def read_config_json(filepath):
    if not os.path.exists(filepath):
        log.error(f"Không tìm thấy file '{filepath}'. Tạo file mẫu...")
        sample_data = [{"app_id": "com.gametree.lhlr.gp", "time_val": "90 Days", "sort_val": "Impression", "max_ads": 100, "start_page": 1}]
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2)
        return []
    
    with open(filepath, 'r', encoding='utf-8') as file:
        raw_tasks = json.load(file)
    
    validated_tasks = []
    for task in raw_tasks:
        if "app_id" not in task:
            continue
        app_id = task["app_id"]
        time_val = task.get("time_val", "90 Days")
        if time_val not in TIME_FILTERS: time_val = "90 Days"
        sort_val = task.get("sort_val", "Impression")
        if sort_val not in SORT_FILTERS and sort_val not in DROPDOWN_SORTS: sort_val = "Impression"
            
        validated_tasks.append({
            "app_id": app_id,
            "networks": task.get("networks", ["youtube"]),
            "time_val": time_val,
            "sort_val": sort_val,
            "max_ads": int(task.get("max_ads", 100)),
            "start_page": int(task.get("start_page", 1)),
            "crawl_page_id": task.get("crawl_page_id", False)
        })
    return validated_tasks

def check_kicked_out(page):
    try:
        if page.locator(Selectors.LOGIN_FORM).is_visible(): return True
        if page.get_by_text(Selectors.KICKED_OUT_MODAL_TEXT, exact=False).is_visible(): return True
    except Exception: pass
    return False

def login_socialpeta(page):
    log.info("=== BẮT ĐẦU ĐĂNG NHẬP LẠI ===")
    show_mouse_cursor(page) 

    email = os.getenv("SP_EMAIL")
    password = os.getenv("SP_PASSWORD")
    if not email or not password:
        log.error("CẢNH BÁO: Thiếu SP_EMAIL hoặc SP_PASSWORD trong file .env. Không thể tự đăng nhập!")
        return False
        
    try:
        if page.get_by_text(Selectors.KICKED_OUT_MODAL_TEXT, exact=False).is_visible():
            human_click(page.get_by_role("button", name="I know"))
            human_wait_with_jitter(page, 1.0, 2.0)
            
        show_mouse_cursor(page) 
        email_input = page.locator(Selectors.LOGIN_EMAIL_INPUT)
        email_input.wait_for(state="visible", timeout=15000)
        human_click(email_input)
        human_type(email_input, email, paste_probability=0.8)
        
        pwd_input = page.locator(Selectors.LOGIN_PASSWORD_INPUT)
        human_click(pwd_input)
        human_type(pwd_input, password, paste_probability=0.8)
        
        human_click(page.locator(Selectors.LOGIN_SUBMIT_BTN))
        log.info("Đã bấm Login, đang chờ vào hệ thống...")
        human_wait_with_jitter(page, 3.0, 5.0)
        log.info("ĐĂNG NHẬP LẠI THÀNH CÔNG! Sẵn sàng khôi phục tiến trình.")
        return True
    except Exception as e:
        log.error(f"Lỗi trong quá trình đăng nhập lại: {e}")
        return False

def view_and_extract_chunk(page, app_data_dict, current_page_number, chunk_start, save_checkpoint_func, crawl_page_id=False, check_cancel_callback=None):
    """
    [KHÔI PHỤC TOẠ ĐỘ]: Tracker chính xác tỷ lệ attempt/success.
    """
    log.info(f"Đang chờ thẻ quảng cáo hiển thị (Trang {current_page_number}). Chuẩn bị quét Chunk từ thẻ {chunk_start}...")
    
    if chunk_start > 0:
        log.info(f"Cuộn trang từ từ xuống vị trí thẻ số {chunk_start + 1} để trang kịp tải hình ảnh/dữ liệu...")
        for _ in range(int(chunk_start / 3) + 1):
            page.mouse.wheel(0, random.randint(800, 1200))
            human_wait_with_jitter(page, 0.4, 0.8)

    ad_cards = page.locator(Selectors.AD_CARD)
    total_available = ad_cards.count()
    
    if chunk_start >= total_available: return False, False, 0, False

    max_ads_required = app_data_dict["scrape_statistics"]["requested_max_ads"]
    current_ads_count = len(app_data_dict["ads"])
    remaining_ads = max_ads_required - current_ads_count
        
    limit = min(5, total_available - chunk_start, remaining_ads)
    human_aimless_highlight(page, probability=0.05)

    chunk_indices = list(range(chunk_start, chunk_start + limit))
    random.shuffle(chunk_indices)

    is_kicked_out_during_extract = False
    needs_reload_during_extract = False 

    attempted_keys = app_data_dict["scrape_statistics"].setdefault("attempted_ad_keys", [])
    for target_ad_idx in chunk_indices:
        ad_key = f"page_{current_page_number}_ad_{target_ad_idx}"
        if ad_key not in attempted_keys:
            attempted_keys.append(ad_key)
    app_data_dict["scrape_statistics"]["total_attempted_ads"] = len(attempted_keys)
    save_checkpoint_func()

    for order_idx, target_ad_idx in enumerate(chunk_indices):
        if check_cancel_callback and check_cancel_callback():
            log.warning("Nhận lệnh HỦY NGANG từ người dùng trong lúc đang bóc tách thẻ.")
            return False, False, 0, True

        max_ads_required = app_data_dict["scrape_statistics"]["requested_max_ads"]
        if len(app_data_dict["ads"]) >= max_ads_required:
            log.info("Đã đạt đủ số lượng max_ads yêu cầu.")
            break

        ad_index_real = target_ad_idx + 1

        already_scraped = any(ad["ad_index"] == ad_index_real and ad["page_number"] == current_page_number for ad in app_data_dict["ads"])
        if already_scraped:
            log.info(f"   + [BỎ QUA] Thẻ số {ad_index_real} đã được cào từ trước. Chuyển thẻ tiếp theo...")
            continue

        if check_kicked_out(page):
            log.warning(f"   [!] PHÁT HIỆN KICK OUT khi đang xử lý thẻ {ad_index_real}. Dừng lập tức!")
            is_kicked_out_during_extract = True
            break

        log.info(f"   + [Trang {current_page_number}] Xử lý thẻ {ad_index_real}...")
        card = ad_cards.nth(target_ad_idx)
        
        try: card.wait_for(state="visible", timeout=LONG_TIMEOUT)
        except Exception: continue
            
        human_smooth_scroll(page, card)

        try:
            skeleton = card.locator(Selectors.SKELETON_LOADER)
            if skeleton.count() > 0: skeleton.first.wait_for(state="hidden", timeout=DEFAULT_TIMEOUT)
            media = card.locator(Selectors.MEDIA_CONTENT).first
            if media.count() > 0: media.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
            else:
                text_content = card.locator(Selectors.ANY_TEXT_DIV).filter(has_text=re.compile(r".+")).first
                text_content.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        except Exception:
            log.debug(f"Bỏ qua chờ thẻ {ad_index_real}, tiếp tục.")

        human_wait_with_jitter(page, 0.1, 0.5)
        human_idle_mouse_move(page, probability=0.1)
        
        modal = page.locator(Selectors.MODAL_CONTENT).first
        popup_opened = False
        for attempt in range(3):
            human_click_safe_zone(card)
            try:
                modal.wait_for(state="visible", timeout=8000)
                popup_opened = True
                break
            except Exception:
                human_delay(0.5, 1.0)

        if popup_opened:
            if check_kicked_out(page):
                log.warning("   [!] PHÁT HIỆN KICK OUT trong lúc đợi Popup render. Ngắt khẩn cấp!")
                is_kicked_out_during_extract = True
                break

            try:
                log.info(f"     -> Mở modal thành công, đang chờ render dữ liệu...")
                skeleton = modal.locator(".ant-skeleton")
                if skeleton.count() > 0:
                    try: skeleton.last.wait_for(state="hidden", timeout=15000)
                    except Exception: pass 
                
                tabs_nav = modal.locator(".ant-tabs-nav").first
                if tabs_nav.count() > 0:
                    try: tabs_nav.wait_for(state="visible", timeout=10000)
                    except Exception: pass
                
                human_wait_with_jitter(page, 0.1, 0.2) 
                if random.random() < 0.05: human_reading_trace(page, modal)
                human_idle_mouse_move(page, probability=0.1)
                
                if crawl_page_id:
                    log.info(f"     -> Chế độ cào Page ID: Đang dò tìm thẻ Modal {ad_index_real}...")
                    
                    try:
                        # 1. Tìm dropdown trigger và di chuột vào
                        dropdown_trigger = modal.locator(Selectors.PAGE_ID_DROPDOWN_TRIGGER).filter(has=page.locator(Selectors.PAGE_ID_DROPDOWN_ICON)).first
                        dropdown_trigger.wait_for(state="visible", timeout=8000)
                        dropdown_trigger.hover()
                        time.sleep(random.uniform(1.0, 1.25))
                        
                        # 2. Tìm menu xổ xuống chứa chữ "Page ID:"
                        page_id_menu_item = page.locator(f'{Selectors.PAGE_ID_MENU_ITEM}:visible').filter(has_text=Selectors.PAGE_ID_LABEL_TEXT).first
                        page_id_menu_item.wait_for(state="visible", timeout=5000)
                        
                        # 3. Lấy text từ thẻ span ngay kế bên thẻ "Page ID:" (Fallback với class .mx-[2px])
                        page_id_text_element = page_id_menu_item.locator(Selectors.PAGE_ID_VALUE_PRIMARY)
                        if page_id_text_element.count() == 0:
                            page_id_text_element = page_id_menu_item.locator(Selectors.PAGE_ID_VALUE_FALLBACK)
                            
                        page_id_value = page_id_text_element.inner_text().strip()
                        
                        if page_id_value:
                            # Tự động lọc trùng (Remove Duplicates)
                            existing_ids = [item["page_id"] for item in app_data_dict.setdefault("page_ids", [])]
                            if page_id_value not in existing_ids:
                                app_data_dict["page_ids"].append({"page_id": page_id_value, "ad_index": ad_index_real})
                                log.info(f"     -> [THÀNH CÔNG] Thu thập được Page ID mới: {page_id_value}")
                            else:
                                log.info(f"     -> [BỎ QUA] Page ID {page_id_value} đã tồn tại.")
                        
                        # Vẫn lưu vết ads để thoả mãn điều kiện vòng lặp max_ads
                        new_ad_data = {
                            "ad_index": ad_index_real,      
                            "process_order": order_idx + 1,    
                            "page_number": current_page_number,
                            "captured_at": datetime.now().isoformat()
                        }
                        app_data_dict["ads"].append(new_ad_data)
                        save_checkpoint_func()

                    except Exception as e:
                        log.error(f"     -> Lỗi khi trích xuất Page ID thẻ {ad_index_real}: {e}")

                else:
                    raw_html = modal.inner_html()
                    new_ad_data = {
                        "ad_index": ad_index_real,      
                        "process_order": order_idx + 1,    
                        "page_number": current_page_number,
                        "captured_at": datetime.now().isoformat(),
                        "raw_html": raw_html
                    }
                    
                    app_data_dict["ads"].append(new_ad_data)
                    log.info(f"     -> [THÀNH CÔNG] Lấy HTML thẻ {ad_index_real} hoàn tất ({len(raw_html)} ký tự).")
                    save_checkpoint_func()

            except Exception as e:
                log.error(f"     -> Lỗi khi lấy nội dung thẻ {ad_index_real}: {e}")
            
            try:
                close_btn = page.get_by_role("button", name="Close")
                human_close_modal(page, close_btn)
                human_retreat_mouse(page)
                human_wait_with_jitter(page, 0.1, 0.3)
            except Exception:
                pass
        else:
            log.warning(f"   [!] CẢNH BÁO: Đã bấm 3 lần vào thẻ {ad_index_real} nhưng Popup không mở. Yêu cầu tải lại toàn bộ trang!")
            needs_reload_during_extract = True
            break 

    return is_kicked_out_during_extract, needs_reload_during_extract, limit, False

# --- CẬP NHẬT HÀM RUN ĐỂ NHẬN 2 THAM SỐ ĐIỀU KHIỂN ---
def run(api_tasks=None, custom_run_id=None, auto_resume=False, time_to_resume=60, 
        status_callback=None, wait_for_user_callback=None, 
        check_cancel_callback=None):
    def report(msg):
        if status_callback: status_callback(msg)

    run_id = custom_run_id or datetime.now().strftime("crawl_%Y%m%d_%H%M%S")
    tasks = read_config_json("crawl_app.json") if api_tasks is None else api_tasks
        
    if not tasks: 
        log.error("Không có dữ liệu task. Hủy chạy.")
        report("Không có dữ liệu task. Hủy chạy.")
        return

    os.makedirs("crawl_json", exist_ok=True)
    output_filename = os.path.join("crawl_json", f"raw_bundle_{run_id}.json")

    if os.path.exists(output_filename):
        with open(output_filename, 'r', encoding='utf-8') as f:
            bundle_data = json.load(f)
        log.info(f"Đã nạp file Checkpoint cũ: {output_filename}. Sẵn sàng khôi phục tiến trình.")
        report(f"Đã nạp file Checkpoint cũ: {output_filename}. Sẵn sàng khôi phục tiến trình.")
    else:
        bundle_data = {"run_id": run_id, "total_apps": len(tasks), "apps": []}

    def do_checkpoint():
        try:
            if "apps" in bundle_data:
                for app in bundle_data["apps"]:
                    collected = len(app.get("ads", []))
                    stats = app.get("scrape_statistics", {})
                    attempted_keys = stats.get("attempted_ad_keys", [])
                    real_attempted = max(len(attempted_keys), collected)
                    stats["total_attempted_ads"] = real_attempted
                    stats["successfully_scraped_ads"] = collected
                    stats["success_rate"] = f"{collected}/{real_attempted}"
                    
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(bundle_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Lỗi khi lưu checkpoint: {e}")
            report(f"Lỗi khi lưu checkpoint: {e}")

    do_checkpoint()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            "./profile-chrome", headless=False, no_viewport=True,
            args=["--disable-blink-features=AutomationControlled", f"--window-size={VIEWPORT_WIDTH},{VIEWPORT_HEIGHT}"]
        )
        page = context.pages[0]
        Stealth().apply_stealth_sync(page)
        page.goto(TARGET_URL)
        human_wait_with_jitter(page, 1.0, 2.5)

        global_kicked_out = False
        stop_reason_msg = ""

        if check_kicked_out(page):
            report("Phát hiện tài khoản bị văng từ trước. Đang đăng nhập...")
            while not login_socialpeta(page):
                if check_cancel_callback and check_cancel_callback():
                    global_kicked_out = True
                    stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình lúc khởi tạo."
                    break
                log.error("KHÔNG THỂ ĐĂNG NHẬP! Thử lại sau 30 giây...")
                report("Đăng nhập lỗi. Thử lại sau 30 giây...")
                for _ in range(30):
                    if check_cancel_callback and check_cancel_callback():
                        global_kicked_out = True
                        break
                    time.sleep(1)
                if global_kicked_out:
                    break
                page.goto(TARGET_URL)

            log.info("Đăng nhập thành công vào Socialpeta")
            report("Đăng nhập thành công")
            page.goto(TARGET_URL)

        show_mouse_cursor(page)

        for index, task in enumerate(tasks):
            if global_kicked_out or (check_cancel_callback and check_cancel_callback()):
                if not global_kicked_out:
                    global_kicked_out = True
                    stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                break

            app_id, time_val, sort_val = task["app_id"], task["time_val"], task["sort_val"]
            max_ads, start_page = task["max_ads"], task["start_page"]
            crawl_page_id = task.get("crawl_page_id", False)
            
            app_data = next((a for a in bundle_data["apps"] if a["app_id"] == app_id), None)
            if not app_data:
                app_data = {
                    "app_id": app_id, "filters_applied": [time_val, sort_val],
                    "scrape_statistics": {"requested_max_ads": max_ads, "total_attempted_ads": 0, "successfully_scraped_ads": 0, "success_rate": "0/0", "report": ""},
                    "last_stopped_page": start_page,
                    "last_processed_chunk_start": 0,
                    "ads": []
                }
                bundle_data["apps"].append(app_data)
                do_checkpoint()
            
            if app_data.get("last_stopped_page") is None and app_data.get("last_processed_chunk_start") is None:
                log.info(f"App {app_id} đã hoàn tất 100% từ trước. Chuyển sang App tiếp theo.")
                report(f"App {app_id} đã hoàn tất 100% từ trước. Chuyển sang App tiếp theo.")
                continue
                
            session_ads_counter = len(app_data["ads"]) % 30 

            app_not_found = False
            search_retry_count = 0
            is_no_data_for_app = False

            while len(app_data["ads"]) < max_ads:
                if check_cancel_callback and check_cancel_callback():
                    global_kicked_out = True
                    stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                    break
                
                # BƯỚC NGOẶT: CHỌN ĐƯỜNG ĐI NẾU BỊ ĐÁ VĂNG
                if check_kicked_out(page):
                    
                    # LUỒNG 1: TỰ ĐỘNG KHÔI PHỤC
                    if auto_resume:
                        log.error(f"!!! MẤT SESSION !!! Nằm im đợi {time_to_resume}s trước khi giành lại nick...")
                        is_cancelled_during_wait = False
                        for i in range(time_to_resume, 0, -1):
                            if check_cancel_callback and check_cancel_callback():
                                is_cancelled_during_wait = True
                                break
                            report(f"Tài khoản bị log in nơi khác. Đang chờ {i} giây để đăng nhập lại...")
                            time.sleep(1)
                            
                        if is_cancelled_during_wait:
                            global_kicked_out = True
                            stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                            break
                        
                        report("Đang tiến hành đăng nhập lại...")
                        # Thử lại vô tận cho đến khi đăng nhập thành công
                        while not login_socialpeta(page):
                            if check_cancel_callback and check_cancel_callback():
                                global_kicked_out = True
                                stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                                break
                            page.goto(TARGET_URL)
                            log.error("KHÔNG THỂ ĐĂNG NHẬP! Thử lại sau 30 giây...")
                            report("Đăng nhập lỗi. Thử lại sau 30 giây...")
                            for _ in range(30):
                                if check_cancel_callback and check_cancel_callback():
                                    global_kicked_out = True
                                    break
                                time.sleep(1)
                            if global_kicked_out:
                                break
                            
                        if global_kicked_out:
                            break
                        page.goto(TARGET_URL)
                        show_mouse_cursor(page)
                        human_wait_with_jitter(page, 2.0, 4.0)
                        continue
                        
                    # LUỒNG 2: HỎI Ý KIẾN NGƯỜI DÙNG HOẶC CHỜ TIMEOUT
                    else:
                        if wait_for_user_callback:
                            current_ads = len(app_data["ads"])
                            log.warning("!!! MẤT SESSION !!! Chuyển trạng thái chờ Frontend ra lệnh...")
                            
                            # Gửi App ID, số ads hiện tại và max ads lên callback
                            action, delay = wait_for_user_callback(app_id, current_ads, max_ads, WAIT_FOR_USER_CALLBACK)
                            
                            if action == "resume":
                                log.error(f"-> Lệnh Resume: Chờ {delay}s trước khi giành lại nick...")
                                is_cancelled_during_wait = False
                                for i in range(delay, 0, -1):
                                    if check_cancel_callback and check_cancel_callback():
                                        is_cancelled_during_wait = True
                                        break
                                    report(f"Sẽ tiến hành đăng nhập lại sau {i} giây...")
                                    time.sleep(1)
                                if is_cancelled_during_wait:
                                    global_kicked_out = True
                                    stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                                    break
                                    
                                report("Đang tiến hành đăng nhập lại...")
                                # Thử lại vô tận cho đến khi đăng nhập thành công
                                while not login_socialpeta(page):
                                    if check_cancel_callback and check_cancel_callback():
                                        global_kicked_out = True
                                        stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                                        break
                                    page.goto(TARGET_URL)
                                    log.error("KHÔNG THỂ ĐĂNG NHẬP! Thử lại sau 10 giây...")
                                    report("Đăng nhập lỗi. Thử lại sau 10 giây...")
                                    for _ in range(10):
                                        if check_cancel_callback and check_cancel_callback():
                                            global_kicked_out = True
                                            break
                                        time.sleep(1)
                                        
                                    if global_kicked_out:
                                        break
                                if global_kicked_out:
                                    break
                                    
                                page.goto(TARGET_URL)
                                show_mouse_cursor(page)
                                human_wait_with_jitter(page, 2.0, 4.0)
                                continue
                                
                            elif action == "stop":
                                log.error("-> Lệnh Stop (Hoặc Timeout): Nhận kết quả ngay. Đóng gói data...")
                                report("Tiếp nhận lệnh NGỪNG. Đang xử lý data đã cào được...")
                                global_kicked_out = True
                                stop_reason_msg = "Người dùng chủ động chọn NGỪNG HOẶC quá thời gian chờ phản hồi 30 phút." 
                                break 
                            else:
                                # Fallback an toàn nếu script chạy tay ngoài console
                                log.error("!!! MẤT SESSION !!! Cờ auto_resume = False -> Lập tức ngắt luồng và trả kết quả.")
                                report("!!! MẤT SESSION !!! Cờ auto_resume = False -> Lập tức ngắt luồng và trả kết quả.")
                                global_kicked_out = True
                                stop_reason_msg = "Tài khoản bị đá văng và tùy chọn auto_resume đang tắt (False)." 
                                break
                        else:
                            # Fallback an toàn nếu script chạy tay ngoài console
                            log.error("!!! MẤT SESSION !!! Cờ auto_resume = False -> Lập tức ngắt luồng và trả kết quả.")
                            report("!!! MẤT SESSION !!! Cờ auto_resume = False -> Lập tức ngắt luồng và trả kết quả.")
                            global_kicked_out = True
                            break

                log.info(f"=== ĐANG XỬ LÝ APP: {app_id} | Đã lấy: {len(app_data['ads'])}/{max_ads} ads ===")
                report(f"=== ĐANG XỬ LÝ APP: {app_id} | Đã lấy: {len(app_data['ads'])}/{max_ads} ads ===")

                human_navigate_to_top(page)
                human_delay(1.0, 1.5)
                
                try:
                    clear_btn = page.locator(Selectors.CLEAR_BTN).filter(has_text="Clear")
                    if clear_btn.count() > 0 and clear_btn.is_visible():
                        human_click(clear_btn.first)
                        human_wait_with_jitter(page, 1.0, 1.5)
                except Exception as e:
                    log.warning(f"Bỏ qua lỗi nút Clear: {e}")
                
                if check_kicked_out(page): continue

                try:
                    input_box = page.locator(Selectors.SEARCH_INPUT)
                    human_click(input_box)
                    human_wait_with_jitter(page, 0.5, 1.0)
                    input_box.press("ControlOrMeta+a", timeout=3000)
                    input_box.press("Backspace", timeout=3000)
                    human_type(input_box, app_id)
                    human_wait_with_jitter(page, 0.5, 2.0) 
                except Exception as e:
                    log.error(f"Lỗi khi tương tác thanh Search ID: {e}. Thử tải lại trang...")
                    page.reload()
                    show_mouse_cursor(page)
                    continue

                log.info(f"Đang chờ tìm kiếm App ID: {app_id}...")
                report(f"Đang chờ tìm kiếm App ID: {app_id}...")
                app_option = page.locator(Selectors.CHOOSE_APP).filter(has_text=app_id).first
                try:
                    app_option.wait_for(state="visible", timeout=15000)
                    human_delay(0.5, 1.0)
                    human_click(app_option)
                    search_retry_count = 0
                except Exception:
                    search_retry_count += 1
                    if search_retry_count >= 3:
                        log.error(f"Đã thử 3 lần nhưng không tìm thấy App {app_id}. Bỏ qua App này.")
                        report(f"Lỗi: Không tìm thấy App {app_id}. Bỏ qua...")
                        app_not_found = True
                        break 
                        
                    log.error(f"Quá 15 giây không tìm thấy App {app_id}. Thử tải lại trang...")
                    report(f"Quá 15 giây không tìm thấy App {app_id}. Thử tải lại trang...")
                    page.reload()
                    show_mouse_cursor(page)
                    continue
                
                human_delay(1.0, 1.5)

                selected_outside = []
                selected_inside = []
                if crawl_page_id:
                    selected_outside = [FACEBOOK_NETWORK_NAME_PAGE_ID]
                    selected_inside = []
                else:
                    networks_requested = task.get("networks", ["youtube"])
                    selected_outside = [n.lower() for n in networks_requested if n.lower() in OUTSIDE_NETWORKS]
                    selected_inside = [n.lower() for n in networks_requested if n.lower() in INSIDE_NETWORKS]

                # 1. XỬ LÝ NHÓM BÊN NGOÀI TRƯỚC (Ví dụ: Tiktok)
                for net in selected_outside:
                    net_label = page.locator(Selectors.get_network_checkbox(net)).first
                    try:
                        net_label.wait_for(state="visible", timeout=5000)
                        net_input = net_label.locator("input.ant-checkbox-input").first
                        if not net_input.is_checked():
                            human_click(net_label)
                            # Delay 1-2s như yêu cầu
                            human_wait_with_jitter(page, 1.0, 2.0) 
                    except Exception as e:
                        log.warning(f"Không thể click chọn mạng {net} ở bên ngoài: {e}")

                if check_kicked_out(page): continue

                # 2. XỬ LÝ NHÓM BÊN TRONG MORE (Ví dụ: Youtube)
                if selected_inside:
                    platform_btn = page.locator(Selectors.PLATFORM_MORE_BTN)
                    if platform_btn.count() > 0 and platform_btn.is_visible():
                        human_click(platform_btn.first)
                        human_wait_with_jitter(page, 1.0, 2.0)
                        
                        for net in selected_inside:
                            net_label = page.locator(Selectors.get_network_checkbox(net)).first
                            try:
                                net_label.wait_for(state="visible", timeout=5000)
                                net_input = net_label.locator("input.ant-checkbox-input").first
                                if not net_input.is_checked():
                                    human_click(net_label)
                                    human_wait_with_jitter(page, 0.5, 1.5)
                            except Exception as e:
                                log.warning(f"Không thể click chọn mạng {net} trong More: {e}")
                                
                        try:
                            ok_btn = page.locator(f'{Selectors.PLATFORM_OK_BTN}:visible').first
                            ok_btn.wait_for(state="visible", timeout=3000)
                            
                            human_click(ok_btn)
                            human_wait_with_jitter(page, 0.5, 1.0)
                        except Exception as e:
                            log.warning(f"Không thấy nút OK hoặc bị lỗi, dùng phím Escape thay thế: {e}")
                            page.keyboard.press("Escape")

                if check_kicked_out(page): continue

                human_click(page.get_by_text(time_val, exact=True).first)
                human_wait_with_jitter(page, 2.0, 4.0)

                if sort_val in DROPDOWN_SORTS:
                    more_btn = page.locator(Selectors.MORE_DROPDOWN_BTN).filter(has=page.locator(Selectors.MORE_ICON))
                    if more_btn.count() > 0:
                        human_click(more_btn.first) 
                        human_delay(0.5, 1.0)
                        sort_option = page.locator(Selectors.DROPDOWN_MENU).get_by_text(sort_val, exact=True)
                        if sort_option.count() > 0: human_click(sort_option.first)
                else:
                    sort_option = page.get_by_text(sort_val, exact=True)
                    if sort_option.count() > 0: human_click(sort_option.first)

                human_retreat_mouse(page)
                human_wait_with_jitter(page, 0.5, 1.0)

                log.info(f"Đang chờ SocialPeta tải dữ liệu kết quả cho App {app_id}...")
                report(f"Đang chờ tải dữ liệu App {app_id}...")
                is_no_data = False
                data_loaded_successfully = False

                for _ in range(30): 
                    if check_cancel_callback and check_cancel_callback():
                        global_kicked_out = True
                        stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                        break
                        
                    if page.locator(Selectors.EMPTY_STATE_CONTAINER).is_visible() or \
                       page.locator(Selectors.EMPTY_STATE_DESCRIPTION).filter(has_text="No data").is_visible():
                        is_no_data = True
                        data_loaded_successfully = True
                        break
                        
                    if page.locator(Selectors.AD_CARD).count() > 0 and page.locator(Selectors.SKELETON_LOADER).count() == 0:
                        time.sleep(1.0)
                        if page.locator(Selectors.SKELETON_LOADER).count() == 0:
                            data_loaded_successfully = True
                            break
                            
                    time.sleep(1)

                if global_kicked_out:
                    break

                if not data_loaded_successfully:
                    log.error(f"   [!] Quá 30 giây vẫn chưa tải xong UI cho App {app_id}. Mạng quá chậm, thử tải lại trang...")
                    page.reload()
                    show_mouse_cursor(page)
                    continue

                if is_no_data:
                    log.warning(f"   [!] App {app_id} không có dữ liệu (No data). Chuyển sang App tiếp theo.")
                    report(f"App {app_id} không có dữ liệu. Bỏ qua.")
                    is_no_data_for_app = True
                    
                    app_data["last_stopped_page"] = None 
                    app_data["last_processed_chunk_start"] = None
                    do_checkpoint()
                    
                    break

                current_ui_page = 1
                target_page = app_data["last_stopped_page"]
                
                if target_page > 1:
                    log.info(f"Đang tua nhanh tới trang đang làm dở: Trang {target_page}...")
                    report(f"Đang tua nhanh tới trang đang làm dở: Trang {target_page}...")
                    human_navigate_to_bottom(page)
                    human_wait_with_jitter(page, 1.0, 2.0)
                    
                    while current_ui_page < target_page:
                        if check_cancel_callback and check_cancel_callback():
                            global_kicked_out = True
                            stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                            break
                        if check_kicked_out(page): break
                        human_wait_with_jitter(page, 0.5, 1.0)
                        
                        skeleton = page.locator(Selectors.SKELETON_LOADER)
                        if skeleton.count() > 0:
                            try: skeleton.first.wait_for(state="hidden", timeout=10000)
                            except Exception: pass
                        
                        next_btn = page.locator(Selectors.NEXT_PAGE_BTN)
                        if next_btn.count() > 0 and next_btn.is_visible():
                            human_click(next_btn.first)
                            current_ui_page += 1
                            
                            active_page_selector = f'li.ant-pagination-item-active[title="{current_ui_page}"]'
                            try:
                                page.locator(active_page_selector).wait_for(state="visible", timeout=15000)
                                log.info(f"  -> UI đã xác nhận load xong trang {current_ui_page}")
                            except Exception:
                                log.warning(f"  -> Cảnh báo: Chờ UI trang {current_ui_page} phản hồi quá 15s!")
                                
                            human_delay(1.0, 2.0)
                        else:
                            log.warning("Không thể bấm Next Page để tua nhanh, có thể đã hết trang.")
                            report("Không thể bấm Next Page để tua nhanh, có thể đã hết trang.")
                            break
                    human_navigate_to_top(page)

                if check_kicked_out(page): continue

                kicked_out_during_scrape = False
                needs_reload_during_scrape = False 
                
                while len(app_data["ads"]) < max_ads:
                    if check_cancel_callback and check_cancel_callback():
                        kicked_out_during_scrape = True
                        global_kicked_out = True
                        stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                        break
                    try: 
                        page.wait_for_selector(Selectors.AD_CARD, state="visible", timeout=DEFAULT_TIMEOUT)
                    except Exception: 
                        if check_kicked_out(page):
                            kicked_out_during_scrape = True
                        break

                    if session_ads_counter >= random.randint(25, 35):
                        pause_time = random.uniform(1.0, 2.0)
                        log.info(f"Nghỉ {pause_time:.1f}s...")
                        report(f"Nghỉ {pause_time:.1f}s...")
                        human_idle_mouse_move(page, probability=0.8)
                        time.sleep(pause_time)
                        session_ads_counter = 0

                    cards_on_page = page.locator(Selectors.AD_CARD).count()
                    if cards_on_page == 0: break
                    
                    chunk_start = app_data["last_processed_chunk_start"]
                    
                    if chunk_start >= cards_on_page:
                        log.info(f"Đã hết thẻ ở Trang {app_data['last_stopped_page']}. Chuyển sang trang tiếp theo.")
                        report(f"Đã hết thẻ ở Trang {app_data['last_stopped_page']}. Chuyển sang trang tiếp theo.")
                        app_data["last_stopped_page"] += 1
                        app_data["last_processed_chunk_start"] = 0
                        do_checkpoint()
                        
                        human_navigate_to_bottom(page)
                        human_wait_with_jitter(page, 0.5, 1.5)
                        
                        if check_kicked_out(page):
                            kicked_out_during_scrape = True
                            break
                            
                        next_page_btn = page.locator(Selectors.NEXT_PAGE_BTN)
                        if next_page_btn.count() > 0 and next_page_btn.is_visible():
                            human_click(next_page_btn.first)
                            next_page_num = app_data["last_stopped_page"]
                            active_page_selector = f'li.ant-pagination-item-active[title="{next_page_num}"]'
                            try:
                                page.locator(active_page_selector).wait_for(state="visible", timeout=15000)
                            except Exception: 
                                pass
                            human_delay(2.0, 3.0)
                            human_navigate_to_top(page)
                            human_delay(1.0, 2.0)
                        else:
                            if check_kicked_out(page):
                                kicked_out_during_scrape = True
                            break
                        continue
                    
                    kicked_out_during_scrape, needs_reload_during_scrape, processed_limit, is_cancelled = view_and_extract_chunk(
                        page, app_data, app_data["last_stopped_page"], chunk_start, do_checkpoint, crawl_page_id, check_cancel_callback
                    )
                    if is_cancelled:
                        global_kicked_out = True
                        stop_reason_msg = "Người dùng chủ động hủy ngang tiến trình."
                        break
                    
                    if kicked_out_during_scrape:
                        break 

                    if needs_reload_during_scrape:
                        log.info("Kích hoạt tiến trình: Tải lại trang và thiết lập lại từ đầu do thẻ bị kẹt...")
                        page.reload()
                        human_wait_with_jitter(page, 3.0, 5.0)
                        break
                        
                    app_data["last_processed_chunk_start"] += processed_limit
                    
                    report(f"Đang ở thẻ số {app_data['last_processed_chunk_start']}, trang số {app_data['last_stopped_page']}, app id {app_data['app_id']}")
                    session_ads_counter += 5
                    do_checkpoint()
                    
                # Xử lý sau khi vòng lặp Chunk bị phá vỡ vì Kicked out
                if kicked_out_during_scrape:
                    continue
                
                if needs_reload_during_scrape:
                    continue

                if global_kicked_out:
                    break
                    
                break

            # CẬP NHẬT REPORT CUỐI CÙNG
            total_ads_collected = len(app_data["ads"])
            
            if global_kicked_out and "hủy ngang" in stop_reason_msg:
                if app_data["scrape_statistics"]["report"] == "":
                    app_data["scrape_statistics"]["report"] = f"Dừng theo yêu cầu người dùng. Đã lấy được {total_ads_collected}/{max_ads} ads."
            elif global_kicked_out:
                app_data["scrape_statistics"]["report"] = f"Đã cào được {total_ads_collected} trên {max_ads} ads. Lý do dừng: {stop_reason_msg}"
            elif app_not_found:
                app_data["scrape_statistics"]["report"] = f"Lỗi: Không tìm thấy App ID này trên hệ thống SocialPeta. Đã tự động bỏ qua."
            elif is_no_data_for_app:
                app_data["scrape_statistics"]["report"] = "Không có dữ liệu (No data) với các bộ lọc hiện tại."
            elif total_ads_collected >= max_ads:
                app_data["scrape_statistics"]["report"] = f"Hoàn hảo! Đã cào thành công toàn bộ {total_ads_collected} trên {max_ads} ads theo đúng yêu cầu."
            else:
                app_data["scrape_statistics"]["report"] = f"Đã cào thành công {total_ads_collected} ads. Lý do dừng: Nền tảng không hiển thị đủ {max_ads} ads như yêu cầu."
            
            # Reset Checkpoint (do_checkpoint cuối cùng để lưu file)
            if not global_kicked_out:
                app_data["last_stopped_page"] = None
                app_data["last_processed_chunk_start"] = None
                
            do_checkpoint()

        log.info(f"Lưu raw data hoàn tất tại: {output_filename}")
        report(f"Lưu raw data hoàn tất tại: {output_filename}")
        context.close()

        is_page_id_run = any(t.get("crawl_page_id", False) for t in tasks)
        if is_page_id_run:
            log.info("CHẾ ĐỘ CÀO PAGE ID: Bỏ qua Gemini và Database. Đang xuất Excel trực tiếp...")
            report("Đang xuất file Excel chứa danh sách Page IDs...")
            excel_path = export_page_id_excel(output_filename)
            if excel_path:
                report("Đã xuất file Excel Page IDs thành công!")
            else:
                report("Hoàn tất, nhưng không có Page ID nào để xuất.")
        else:
            report("Đang gửi dữ liệu thô sang Gemini AI để bóc tách nội dung...")
            log.info("CHUYỂN GIAO SANG GEMINI PARSER...")
            report("CHUYỂN GIAO SANG GEMINI PARSER...")
            api_key = os.getenv("GEMINI_API_KEY")
            if api_key: 
                final_json_path = process_bundle(output_filename, api_key, DEFAULT_MODEL)
                if final_json_path:
                    log.info("CHUYỂN GIAO SANG EXCEL EXPORTER...")
                    report("Đang xuất file Excel và cập nhật Database...")
                    json_to_excel(final_json_path)
                else:
                    log.error("Không có file final JSON để chuyển sang Excel.")
                    report("Không có file final JSON để chuyển sang Excel.")

if __name__ == "__main__":
    run()