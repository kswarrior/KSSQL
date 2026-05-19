import time
from playwright.sync_api import sync_playwright

def run(url, name, is_mobile=False):
    print(f"Verifying {name} UI at {url} (Mobile: {is_mobile})...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        if is_mobile:
            context = browser.new_context(viewport={'width': 375, 'height': 812}, is_mobile=True)
        else:
            context = browser.new_context(viewport={'width': 1920, 'height': 1080})
            
        page = context.new_page()
        try:
            page.goto(url)
            time.sleep(2)
            page.screenshot(path=f"screenshot_v2_{name}.png", full_page=True)
            print(f"Successfully captured {name} screenshot.")
        except Exception as e:
            print(f"Failed to capture {name}: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    # Note: Using target/debug/ks-sql from previous builds
    import os
    os.system("./target/debug/ks-sql --port w:8080 m:5432 > server_v2_ui.log 2>&1 &")
    time.sleep(5)
    try:
        run("http://localhost:8080/", "desktop")
        run("http://localhost:8080/", "mobile", is_mobile=True)
    finally:
        os.system("kill $(pgrep ks-sql)")
