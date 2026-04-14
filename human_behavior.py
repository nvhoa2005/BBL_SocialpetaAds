import time
import random
import pyperclip
from custom_logger import log
from constants import VIEWPORT_WIDTH, VIEWPORT_HEIGHT

last_mouse_x = random.choice([0, VIEWPORT_WIDTH])
last_mouse_y = random.uniform(100, 800)

def get_bezier_curve(p0, p1, p2, p3, num_points=30):
    points = []
    for t in [i / num_points for i in range(num_points + 1)]:
        x = (1 - t)**3 * p0[0] + 3 * (1 - t)**2 * t * p1[0] + 3 * (1 - t) * t**2 * p2[0] + t**3 * p3[0]
        y = (1 - t)**3 * p0[1] + 3 * (1 - t)**2 * t * p1[1] + 3 * (1 - t) * t**2 * p2[1] + t**3 * p3[1]
        points.append((x, y))
    return points

def move_mouse_with_bezier(page, target_x, target_y):
    """
    Nâng cấp Fitts's Law: Đã được tinh chỉnh để di chuyển NHANH HƠN.
    """
    global last_mouse_x, last_mouse_y
    
    is_overshoot = random.random() < 0.3
    final_x, final_y = target_x, target_y
    if is_overshoot:
        # Giảm khoảng cách văng lố để thu chuột về nhanh hơn
        target_x += random.uniform(-15, 15) 
        target_y += random.uniform(-15, 15)

    cp1_x = last_mouse_x + random.uniform(-150, 150)
    cp1_y = last_mouse_y + random.uniform(-150, 150)
    cp2_x = target_x + random.uniform(-100, 100)
    cp2_y = target_y + random.uniform(-100, 100)
    
    # 1. TĂNG TỐC TỔNG THỂ: Giảm số điểm vẽ cong (từ 20-40 xuống 12-22)
    curve_points = get_bezier_curve(
        (last_mouse_x, last_mouse_y), (cp1_x, cp1_y), (cp2_x, cp2_y), (target_x, target_y), 
        num_points=random.randint(12, 22) 
    )
    
    num_points = len(curve_points)
    for i, (px, py) in enumerate(curve_points):
        page.mouse.move(px, py)
        progress = i / num_points
        
        # 2. TĂNG GIA TỐC: Giảm độ trễ cực nhỏ giữa mỗi nhịp di chuyển
        if progress < 0.2 or progress > 0.8:
            time.sleep(random.uniform(0.003, 0.007)) # Đầu/cuối: Giảm một nửa thời gian hãm phanh
        else:
            time.sleep(random.uniform(0.001, 0.002)) # Khúc giữa: Phóng cực nhanh (1-2 ms)

    if is_overshoot:
        # Thời gian nhận ra mình kéo lố giảm xuống
        time.sleep(random.uniform(0.05, 0.1)) 
        page.mouse.move(final_x, final_y, steps=random.randint(2, 4))

    last_mouse_x = final_x
    last_mouse_y = final_y

def human_idle_mouse_move(page, probability=0.3):
    if random.random() < probability:
        log.debug("Kích hoạt hành vi: Lia chuột vẩn vơ.")
        for _ in range(random.randint(1, 3)):
            target_x = random.uniform(VIEWPORT_WIDTH * 0.15, VIEWPORT_WIDTH * 0.85)
            target_y = random.uniform(VIEWPORT_HEIGHT * 0.15, VIEWPORT_HEIGHT * 0.85)
            move_mouse_with_bezier(page, target_x, target_y)
            time.sleep(random.uniform(0.3, 0.8))

def human_aimless_highlight(page, probability=0.2):
    """
    Hành vi bôi đen text vô thức trên màn hình rồi hủy bằng phím mũi tên (Rất an toàn, không sợ click nhầm link).
    """
    if random.random() < probability:
        log.debug("Kích hoạt hành vi: Bôi đen text vô thức.")
        viewport = page.viewport_size
        start_x = random.uniform(VIEWPORT_WIDTH * 0.2, VIEWPORT_WIDTH * 0.8)
        start_y = random.uniform(VIEWPORT_HEIGHT * 0.2, VIEWPORT_HEIGHT * 0.8)
        
        move_mouse_with_bezier(page, start_x, start_y)
        page.mouse.down()
        time.sleep(random.uniform(0.1, 0.3))
        
        # Kéo ngang một đoạn
        end_x = start_x + random.uniform(100, 300)
        end_y = start_y + random.uniform(-10, 20)
        page.mouse.move(end_x, end_y, steps=random.randint(10, 20))
        time.sleep(random.uniform(0.2, 0.5))
        page.mouse.up()
        
        # Hủy bôi đen bằng phím điều hướng
        time.sleep(random.uniform(0.5, 1.5))
        page.keyboard.press("ArrowDown")
        time.sleep(random.uniform(0.1, 0.3))
        page.keyboard.press("ArrowUp")

def human_click_safe_zone(locator):
    try:
        box = locator.bounding_box(timeout=3000)
        if box:
            x, y, width, height = box['x'], box['y'], box['width'], box['height']
            target_x = x + random.uniform(width * 0.20, width * 0.30)
            target_y = y + random.uniform(height * 0.30, height * 0.65)
            move_mouse_with_bezier(locator.page, target_x, target_y)
            time.sleep(random.uniform(0.1, 0.2)) 
            locator.page.mouse.click(target_x, target_y, delay=random.randint(20, 50))
        else:
            locator.click(delay=random.randint(20, 50), timeout=3000)
    except Exception as e:
        log.warning(f"Bỏ qua lỗi human_click_safe_zone: {e}")

def human_click(locator):
    try:
        box = locator.bounding_box(timeout=3000)
        if box:
            x, y, width, height = box['x'], box['y'], box['width'], box['height']
            target_x = x + random.uniform(width * 0.20, width * 0.80)
            target_y = y + random.uniform(height * 0.40, height * 0.60)
            move_mouse_with_bezier(locator.page, target_x, target_y)
            time.sleep(random.uniform(0.1, 0.3))  
            locator.page.mouse.click(target_x, target_y, delay=random.randint(20, 50))
        else:
            locator.click(delay=random.randint(20, 50), timeout=3000)  
    except Exception as e:
        log.warning(f"Bỏ qua lỗi human_click: {e}")

def human_type(locator, text, paste_probability=0.9):
    try:
        box = locator.bounding_box(timeout=3000)
        if box:
            target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
            target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
            locator.page.mouse.click(target_x, target_y)
    except Exception as e:
        log.debug(f"Không thể click lấy focus trong human_type (vẫn tiếp tục gõ): {e}")
        
    time.sleep(random.uniform(1.0, 2.0))
    keyboard = locator.page.keyboard

    if random.random() < paste_probability:
        pyperclip.copy(text)
        time.sleep(random.uniform(0.3, 0.8))
        keyboard.press("ControlOrMeta+v")
        time.sleep(random.uniform(0.4, 0.8))
        return
    
    nearby_keys = {
        'a': ['s', 'q'], 'b': ['v', 'n'], 'c': ['x', 'v'], 'd': ['s', 'f'],
        'e': ['w', 'r'], 'f': ['d', 'g'], 'g': ['f', 'h'], 'h': ['g', 'j'],
        'i': ['u', 'o'], 'j': ['h', 'k'], 'k': ['j', 'l'], 'l': ['k'],
        'm': ['n'], 'n': ['b', 'm'], 'o': ['i', 'p'], 'p': ['o'],
        'q': ['a', 'w'], 'r': ['e', 't'], 's': ['a', 'd'], 't': ['r', 'y'],
        'u': ['y', 'i'], 'v': ['c', 'b'], 'w': ['q', 'e'], 'x': ['z', 'c'],
        'y': ['t', 'u'], 'z': ['x'], '.': [',', '/']
    }

    for char in text:
        if char.lower() in nearby_keys and random.random() < 0.20:
            wrong_char = random.choice(nearby_keys[char.lower()])
            if char.isupper(): wrong_char = wrong_char.upper()
            keyboard.type(wrong_char, delay=random.randint(50, 100))
            time.sleep(random.uniform(0.4, 0.7)) 
            keyboard.press("Backspace")
            time.sleep(random.uniform(0.2, 0.4))

        keyboard.type(char, delay=random.randint(50, 150))
        time.sleep(random.uniform(0.01, 0.05))

def human_smooth_scroll(page, locator):
    try:
        # Ép timeout 3 giây. Nếu thẻ bị ẩn/xóa, nó quăng lỗi ngay chứ không treo 30s.
        box = locator.bounding_box(timeout=3000)
        if not box: 
            locator.scroll_into_view_if_needed(timeout=3000)
            time.sleep(0.5)
            box = locator.bounding_box(timeout=3000)
            if not box: return
        
        target_y = box['y']
        safe_top = VIEWPORT_HEIGHT * 0.4
        safe_bottom = VIEWPORT_HEIGHT * 0.6
        
        if target_y > safe_top and target_y < safe_bottom:
            return 

        move_mouse_with_bezier(page, VIEWPORT_WIDTH * 0.5, VIEWPORT_HEIGHT * 0.5)
        time.sleep(random.uniform(0.1, 0.3))

        distance_to_scroll = target_y - (VIEWPORT_HEIGHT * random.uniform(0.4, 0.6)) 
        is_overscroll = random.random() < 0.3
        if is_overscroll:
            distance_to_scroll += random.uniform(100, 250)

        steps = random.randint(12, 20)
        step_distance = distance_to_scroll / steps
        
        for _ in range(steps):
            page.mouse.wheel(0, step_distance + random.uniform(-10, 15))
            time.sleep(random.uniform(0.03, 0.09)) 
            if random.random() < 0.05:
                time.sleep(random.uniform(0.8, 1.5))

        if is_overscroll:
            time.sleep(random.uniform(0.4, 0.8))
            page.mouse.wheel(0, -random.uniform(100, 250))
            
        time.sleep(random.uniform(0.4, 0.8))

        new_box = locator.bounding_box(timeout=3000)
        if new_box and (new_box['y'] < safe_top or new_box['y'] > safe_bottom):
            locator.scroll_into_view_if_needed(timeout=3000)
            time.sleep(0.5)
    except Exception as e:
        log.warning(f"Bỏ qua lỗi human_smooth_scroll (Thẻ có thể đã bị DOM gỡ bỏ): {e}")

def human_delay(min_seconds=1.0, max_seconds=3.0):
    time.sleep(random.uniform(min_seconds, max_seconds))

def show_mouse_cursor(page):
    js_code = """
    () => {
        const box = document.createElement('div');
        box.style.cssText = 'pointer-events: none; position: absolute; z-index: 10000; width: 20px; height: 20px; background: rgba(255, 0, 0, 0.4); border: 2px solid red; border-radius: 50%; margin: -10px 0 0 -10px; transition: transform 0.1s ease-out;';
        document.body.appendChild(box);
        document.addEventListener('mousemove', (e) => { box.style.left = e.pageX + 'px'; box.style.top = e.pageY + 'px'; });
        document.addEventListener('mousedown', () => { box.style.transform = 'scale(0.5)'; box.style.background = 'rgba(0, 255, 0, 0.8)'; });
        document.addEventListener('mouseup', () => { box.style.transform = 'scale(1)'; box.style.background = 'rgba(255, 0, 0, 0.4)'; });
    }
    """
    page.evaluate(js_code)

def human_wait_with_jitter(page, min_seconds=1.0, max_seconds=3.0):
    """
    Thay thế cho time.sleep(). Chuột sẽ rung lắc nhẹ (nhích vài pixel) trong lúc chờ đợi 
    để mô phỏng tay người cầm chuột không bao giờ đứng yên.
    """
    global last_mouse_x, last_mouse_y
    wait_time = random.uniform(min_seconds, max_seconds)
    end_time = time.time() + wait_time
    
    while time.time() < end_time:
        if random.random() < 0.4:  # 40% cơ hội nhúc nhích
            jitter_x = last_mouse_x + random.uniform(-3, 3)
            jitter_y = last_mouse_y + random.uniform(-3, 3)
            
            # Đảm bảo không văng ra khỏi màn hình
            viewport = page.viewport_size
            if viewport:
                jitter_x = max(0, min(viewport['width'], jitter_x))
                jitter_y = max(0, min(viewport['height'], jitter_y))
                
            page.mouse.move(jitter_x, jitter_y)
            last_mouse_x, last_mouse_y = jitter_x, jitter_y
            
        time.sleep(random.uniform(0.1, 0.3))

def human_reading_trace(page, locator):
    try:
        box = locator.bounding_box(timeout=3000)
        if not box: return
        start_x = box['x'] + box['width'] * random.uniform(0.1, 0.3)
        end_x = box['x'] + box['width'] * random.uniform(0.7, 0.9)
        y_pos = box['y'] + box['height'] * random.uniform(0.2, 0.8)
        
        move_mouse_with_bezier(page, start_x, y_pos)
        human_wait_with_jitter(page, 0.3, 0.8)
        move_mouse_with_bezier(page, end_x, y_pos + random.uniform(-15, 15))
    except Exception as e:
        log.debug(f"Bỏ qua lỗi human_reading_trace: {e}")

def human_retreat_mouse(page):
    # Cất chuột ra vùng an toàn: 15-25% lề trái, hoặc 75-85% lề phải
    target_x = random.choice([
        random.uniform(VIEWPORT_WIDTH * 0.15, VIEWPORT_WIDTH * 0.25),  
        random.uniform(VIEWPORT_WIDTH * 0.75, VIEWPORT_WIDTH * 0.85) 
    ])
    target_y = random.uniform(VIEWPORT_HEIGHT * 0.2, VIEWPORT_HEIGHT * 0.8)
    
    log.debug("Kích hoạt hành vi: Cất chuột ra vùng an toàn.")
    move_mouse_with_bezier(page, target_x, target_y)

def human_navigate_to_top(page, probability_scroll=1.0):
    """
    Tỷ lệ 70% cuộn chuột lên đầu trang (nhiều nấc), 30% bấm phím Home.
    Đã bổ sung cơ chế chống lỗi cuộn mượt (Smooth Scroll) và chống kẹt.
    """
    if random.random() < probability_scroll:
        log.debug("Hành vi: Cuộn chuột lên đầu trang.")
        current_y = page.evaluate("window.scrollY")
        stuck_count = 0
        
        while current_y > 0:
            scroll_step = -random.randint(3, 7) * 120
            page.mouse.wheel(0, scroll_step)
            time.sleep(random.uniform(0.1, 0.25)) # Tăng thời gian chờ để trình duyệt kịp render
            
            new_y = page.evaluate("window.scrollY")
            
            # Làm tròn và cho phép chênh lệch nhỏ (5 pixels)
            if abs(new_y - current_y) < 5 or new_y <= 0: 
                stuck_count += 1
                if stuck_count >= 3 or new_y <= 0: # Thử 3 lần vẫn kẹt hoặc đã đến đỉnh
                    break
                time.sleep(random.uniform(0.2, 0.4)) # Chờ thêm một chút xem trang có giật lên không
            else:
                stuck_count = 0 # Reset nếu vẫn cuộn bình thường
                
            current_y = new_y
    else:
        log.debug("Hành vi: Bấm phím Home.")
        page.keyboard.press("Home")
        # Phải chờ sau khi bấm phím để UI kịp dịch chuyển trước khi code chạy tiếp
        time.sleep(random.uniform(0.8, 1.5)) 

def human_navigate_to_bottom(page, probability_scroll=1.0):
    """
    Tỷ lệ 70% cuộn chuột xuống cuối trang, 30% bấm phím End.
    Đã bổ sung cơ chế chống lỗi cuộn mượt (Smooth Scroll) và chờ Lazy Loading.
    """
    if random.random() < probability_scroll:
        log.debug("Hành vi: Cuộn chuột xuống cuối trang.")
        current_y = page.evaluate("window.scrollY")
        stuck_count = 0
        
        while True:
            scroll_step = random.randint(3, 7) * 120
            page.mouse.wheel(0, scroll_step)
            time.sleep(random.uniform(0.1, 0.25))
            
            new_y = page.evaluate("window.scrollY")
            
            # Làm tròn và cho phép chênh lệch nhỏ để tránh lỗi sub-pixel
            if abs(new_y - current_y) < 5: 
                stuck_count += 1
                # Nếu kẹt 3 lần liên tiếp (chờ load thanh phân trang hoặc thực sự chạm đáy)
                if stuck_count >= 3: 
                    break
                # Càng kẹt càng chờ lâu hơn một chút (mô phỏng người dùng đợi trang load)
                time.sleep(random.uniform(0.4, 0.7)) 
            else:
                stuck_count = 0 
                
            current_y = new_y
    else:
        log.debug("Hành vi: Bấm phím End.")
        page.keyboard.press("End")
        # Rất quan trọng: Bấm End xong phải chờ trang tải xong các element ẩn ở đáy
        time.sleep(random.uniform(1.0, 2.0))

def human_close_modal(page, close_btn_locator=None):
    """
    Đóng popup quảng cáo theo 3 cách ngẫu nhiên để giả lập thói quen người dùng:
    - 40% click vào vùng xám (overlay mask) bên ngoài popup.
    - 40% click vào nút Close (X).
    - 20% bấm phím Escape trên bàn phím.
    """
    choice = random.random()
    
    if choice < 0.4:
        log.debug("Hành vi: Click ra vùng xám ngoài popup để đóng.")
        viewport = page.viewport_size
        if viewport:
            # Chọn tọa độ vùng xám an toàn: lề trái ngoài cùng hoặc lề phải ngoài cùng
            target_x = random.choice([
                random.uniform(10, 80), 
                random.uniform(viewport['width'] - 80, viewport['width'] - 10)
            ])
            target_y = random.uniform(viewport['height'] * 0.2, viewport['height'] * 0.8)
            
            move_mouse_with_bezier(page, target_x, target_y)
            time.sleep(random.uniform(0.2, 0.6))
            page.mouse.click(target_x, target_y, delay=random.randint(50, 150))
        else:
            page.keyboard.press("Escape")
            
    elif choice < 0.8 and close_btn_locator and close_btn_locator.count() > 0:
        log.debug("Hành vi: Click nút Close.")
        # Dùng lại hàm human_click để có đường cong di chuột
        human_click(close_btn_locator.first)
        
    else:
        log.debug("Hành vi: Bấm phím Escape.")
        # Rút chuột ra mép trước khi bấm phím cho giống thật
        target_x = random.uniform(10, 100)
        target_y = random.uniform(100, 500)
        move_mouse_with_bezier(page, target_x, target_y)
        time.sleep(random.uniform(0.2, 0.5))
        page.keyboard.press("Escape")