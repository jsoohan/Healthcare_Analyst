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


def _create_stealth_driver(headless, download_dir, chrome_profile, profile_dir):
    """Use undetected-chromedriver for anti-detection."""
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

    driver = uc.Chrome(options=options, use_subprocess=True)
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


def create_driver(headless=False, download_dir=None, stealth=None,
                   chrome_profile=None, profile_dir=None):
    """Create a Chrome driver with optional anti-detection.

    Args:
        headless: Run without visible window (default False — captchas easier to solve with window)
        download_dir: Directory to download files to
        stealth: Use undetected-chromedriver. Defaults to STEALTH_BROWSER env var.
        chrome_profile: Path to Chrome user data dir. Defaults to CHROME_PROFILE env.
        profile_dir: Profile subdirectory name (usually "Default"). Defaults to CHROME_PROFILE_DIR env or "Default".
    """
    if stealth is None:
        stealth = os.getenv("STEALTH_BROWSER", "").lower() in ("1", "true", "yes")
    if chrome_profile is None:
        chrome_profile = os.getenv("CHROME_PROFILE") or None
    if profile_dir is None:
        profile_dir = os.getenv("CHROME_PROFILE_DIR", "Default")

    if stealth:
        return _create_stealth_driver(headless, download_dir, chrome_profile, profile_dir)
    return _create_regular_driver(headless, download_dir, chrome_profile, profile_dir)
