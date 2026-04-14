from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        user_data_dir = "./profile-chrome"

        print("Đang mở trình duyệt...")

        context = p.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,  
            viewport={"width": 1280, "height": 720}
        )

        page = context.pages[0]
        page.goto("https://socialpeta.com/modules/creative/display-ads")
        page.pause()
        

if __name__ == "__main__":
    run()