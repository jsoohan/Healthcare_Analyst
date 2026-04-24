#!/usr/bin/env python3
"""
Shared Chrome driver factory with anti-detection options.

Usage:
    from scripts.browser_utils import create_driver

    # Default: regular Selenium Chrome
    driver = create_driver()

    # Anti-detection: undetected-chromedriver
    driver = create_driver(stealth=True)

    # Use real Chrome profile (preserves cookies, history, looks like human)
    driver = create_driver(
        chrome_profile=r"C:/Users/<name>/AppData/Local/Google/Chrome/User Data",
        profile_dir="Default",
    )

Environment variables:
    STEALTH_BROWSER=1 -> force undetected-chromedriver
    CHROME_PROFILE=<path> -> Chrome user data dir (all scripts)
    CHROME_PROFILE_DIR=<name> -> Profile name (default "Default")
"""
import os
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def _build_options(headless=False, download_dir=None, chrome_profile=None,
                    profile_dir="Default"):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # Use real Chrome profile (major anti-detection boost)
    if chrome_profile:
        options.add_argument(f"--user-data-dir={chrome_profile}")
        options.add_argument(f"--profile-directory={profile_dir}")

    if download_dir:
        abs_dir = os.path.abspath(download_dir)
        os.makedirs(abs_dir, exist_ok=True)
        options.add_experimental_option("prefs", {
            "download.default_directory": abs_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
        })
    return options


def _detect_chrome_major_version():
    """Detect installed Chrome major version on Windows/macOS/Linux."""
    import re as _re
    import subprocess
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                # Windows
                if path.endswith(".exe"):
                    out = subprocess.check_output(
                        ["wmic", "datafile", "where",
                         f'name="{path.replace(chr(92), chr(92)*2)}"',
                         "get", "Version", "/value"],
                        stderr=subprocess.DEVNULL, timeout=5
                    ).decode(errors="ignore")
                else:
                    out = subprocess.check_output(
                        [path, "--version"], timeout=5
                    ).decode(errors="ignore")
                m = _re.search(r"(\d+)\.", out)
                if m:
                    return int(m.group(1))
            except Exception:
                continue
    return None


def _create_stealth_driver(headless, download_dir, chrome_profile, profile_dir):
    """Use undetected-chromedriver for anti-detection.

    If it fails, prints diagnostic info and re-raises.
    """
    try:
        import undetected_chromedriver as uc
    except ImportError:
        raise RuntimeError(
            "undetected-chromedriver not installed. Run: "
            "pip install undetected-chromedriver"
        )

    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    if chrome_profile:
        options.add_argument(f"--user-data-dir={chrome_profile}")
        options.add_argument(f"--profile-directory={profile_dir}")

    # Try to match Chrome version explicitly (prevents driver/browser mismatch)
    version_main = _detect_chrome_major_version()

    kwargs = {"options": options}
    if version_main:
        kwargs["version_main"] = version_main

    try:
        driver = uc.Chrome(**kwargs)
    except Exception as e:
        msg = str(e)
        hint = []
        if "cannot connect to chrome" in msg.lower() or "session not created" in msg.lower():
            hint = [
                "\n[TROUBLESHOOT] undetected-chromedriver failed to start Chrome.",
                "Common causes:",
                "  1. Chrome is already running with the same profile.",
                "     -> Close ALL Chrome windows before running this script.",
                "  2. Chrome version mismatch.",
                f"     -> Detected major version: {version_main or '(unknown)'}",
                "     -> Update Chrome, or pip install -U undetected-chromedriver",
                "  3. Profile path is invalid or read-only.",
                f"     -> CHROME_PROFILE={chrome_profile}",
                "     -> Try a dedicated scraping profile (see docs/PIPELINE.md).",
                "",
                "Fallback: unset STEALTH_BROWSER and CHROME_PROFILE to use regular Selenium:",
                "  Remove-Item Env:STEALTH_BROWSER",
                "  Remove-Item Env:CHROME_PROFILE",
            ]
        raise RuntimeError("\n".join([msg] + hint)) from e

    if download_dir:
        abs_dir = os.path.abspath(download_dir)
        os.makedirs(abs_dir, exist_ok=True)
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": abs_dir,
        })
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(15)
    driver.implicitly_wait(5)
    return driver


def _create_regular_driver(headless, download_dir, chrome_profile, profile_dir):
    """Standard selenium Chrome with anti-detection tweaks."""
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager

    options = _build_options(headless, download_dir, chrome_profile, profile_dir)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(15)
    driver.implicitly_wait(5)
    try:
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
    except Exception:
        pass
    if download_dir and headless:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(download_dir),
        })
    return driver


def _create_edge_driver(headless, download_dir, chrome_profile, profile_dir):
    """Microsoft Edge driver (Chromium-based). Best paired with Bing search."""
    from selenium.webdriver.edge.service import Service as EdgeService
    from selenium.webdriver.edge.options import Options as EdgeOptions

    options = EdgeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    if chrome_profile:
        options.add_argument(f"--user-data-dir={chrome_profile}")
        options.add_argument(f"--profile-directory={profile_dir}")
    if download_dir:
        abs_dir = os.path.abspath(download_dir)
        os.makedirs(abs_dir, exist_ok=True)
        options.add_experimental_option("prefs", {
            "download.default_directory": abs_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        })

    try:
        from webdriver_manager.microsoft import EdgeChromiumDriverManager
        service = EdgeService(EdgeChromiumDriverManager().install())
    except Exception:
        service = EdgeService()

    driver = webdriver.Edge(service=service, options=options)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(15)
    driver.implicitly_wait(5)
    return driver


def create_driver(headless=False, download_dir=None, stealth=None,
                   chrome_profile=None, profile_dir=None, browser=None):
    """Create a browser driver with optional anti-detection.

    Args:
        browser: "chrome" (default), "edge", or env var BROWSER.
                 Edge + Bing = Microsoft stack = virtually no captcha.
    """
    if browser is None:
        browser = os.getenv("BROWSER", "chrome").lower()
    if stealth is None:
        stealth = os.getenv("STEALTH_BROWSER", "").lower() in ("1", "true", "yes")
    if chrome_profile is None:
        chrome_profile = os.getenv("CHROME_PROFILE") or None
    if profile_dir is None:
        profile_dir = os.getenv("CHROME_PROFILE_DIR", "Default")

    if browser == "edge":
        return _create_edge_driver(headless, download_dir,
                                     chrome_profile, profile_dir)

    if stealth:
        try:
            return _create_stealth_driver(headless, download_dir,
                                            chrome_profile, profile_dir)
        except Exception as e:
            print(f"  [WARN] Stealth driver failed, falling back to regular Selenium")
            print(f"         ({str(e)[:100]})")
            # Kill any orphaned chrome processes before fallback
            _kill_chrome_processes()
            import time
            time.sleep(3)
            try:
                return _create_regular_driver(headless, download_dir,
                                                chrome_profile, profile_dir)
            except Exception as e2:
                print(f"  [WARN] Regular driver with profile also failed, "
                      f"trying without profile")
                return _create_regular_driver(headless, download_dir,
                                                None, "Default")

    return _create_regular_driver(headless, download_dir,
                                    chrome_profile, profile_dir)


def _kill_chrome_processes():
    """Kill lingering Chrome/chromedriver processes to free ports."""
    import subprocess, sys as _sys
    if _sys.platform.startswith("win"):
        subprocess.run("taskkill /F /IM chromedriver.exe >nul 2>&1", shell=True)
        subprocess.run("taskkill /F /IM chrome.exe >nul 2>&1", shell=True)
    else:
        subprocess.run("pkill -f chromedriver 2>/dev/null", shell=True)
        subprocess.run("pkill -f 'Google Chrome' 2>/dev/null", shell=True)
