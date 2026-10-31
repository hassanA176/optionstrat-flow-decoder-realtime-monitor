from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import (
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)


LOGIN_URL = "https://optionstrat.com/login"
FLOW_URL = "https://optionstrat.com/flow/live"

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "session"
COOKIES_FILE = SESSION_DIR / "cookies.json"
STORAGE_STATE_FILE = SESSION_DIR / "storage_state.json"
SESSION_INFO_FILE = SESSION_DIR / "session_info.json"


class LoginError(RuntimeError):
    """Raised when the OptionStrat login could not be completed."""


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_credentials() -> tuple[str, str]:
    load_dotenv(BASE_DIR / ".env")

    email = os.getenv("OPTIONSTRAT_EMAIL", "").strip()
    password = os.getenv("OPTIONSTRAT_PASSWORD", "").strip()

    if not email or not password:
        raise LoginError(
            "ضع OPTIONSTRAT_EMAIL و OPTIONSTRAT_PASSWORD داخل ملف .env أولاً."
        )

    return email, password


async def click_login_button(page: Page) -> None:
    """Click the login button using a few safe fallbacks."""
    candidates = [
        page.locator('button[type="submit"]'),
        page.get_by_role(
            "button",
            name=re.compile(r"^(log\s*in|login|sign\s*in)$", re.IGNORECASE),
        ),
        page.locator('input[type="submit"]'),
    ]

    for locator in candidates:
        if await locator.count() > 0:
            await locator.first.click()
            return

    # The form normally submits when Enter is pressed in the password field.
    await page.locator('input[name="password"]').press("Enter")


async def wait_for_login_success(page: Page, timeout_ms: int = 90_000) -> None:
    """Wait until the login page is left, then verify the protected flow page opens."""
    try:
        await page.wait_for_function(
            "() => !location.pathname.startsWith('/login')",
            timeout=timeout_ms,
        )
    except PlaywrightTimeoutError as exc:
        raise LoginError(
            "لم يتم تأكيد تسجيل الدخول. افحص البريد وكلمة المرور، وأكمل أي تحقق ظاهر في المتصفح."
        ) from exc

    # Open the protected page to make sure the saved session is usable.
    response = await page.goto(FLOW_URL, wait_until="domcontentloaded", timeout=60_000)

    if response is not None and response.status >= 400:
        raise LoginError(
            f"تمت محاولة فتح صفحة Flow لكن الخادم أعاد الحالة HTTP {response.status}."
        )

    await page.wait_for_timeout(2_000)

    if "/login" in page.url:
        raise LoginError("أعاد الموقع التوجيه إلى صفحة الدخول؛ الجلسة لم تُحفظ بنجاح.")


async def save_session(context: BrowserContext, page: Page) -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)

    # Save every cookie currently available in this browser context.
    cookies = await context.cookies()
    COOKIES_FILE.write_text(
        json.dumps(cookies, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Save cookies + localStorage in Playwright's native format as a backup.
    await context.storage_state(path=str(STORAGE_STATE_FILE))

    session_info = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "current_url": page.url,
        "cookie_count": len(cookies),
        "cookies_file": str(COOKIES_FILE),
        "storage_state_file": str(STORAGE_STATE_FILE),
    }
    SESSION_INFO_FILE.write_text(
        json.dumps(session_info, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\n✅ تم حفظ الجلسة بنجاح")
    print(f"🍪 عدد الكوكيز: {len(cookies)}")
    print(f"📄 Cookies: {COOKIES_FILE}")
    print(f"📄 Storage state: {STORAGE_STATE_FILE}")


async def main() -> None:
    email, password = load_credentials()
    headless = _env_bool("OPTIONSTRAT_HEADLESS", default=False)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=headless,
        )

        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
        )

        page = await context.new_page()

        try:
            print("🌐 فتح صفحة تسجيل الدخول...")
            await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)

            print("✍️ إدخال بيانات الحساب...")
            await page.locator('input[name="email"]').fill(email)
            await page.locator('input[name="password"]').fill(password)

            print("🔐 تنفيذ تسجيل الدخول...")
            await click_login_button(page)
            await wait_for_login_success(page)
            await save_session(context, page)

            print("\nيمكنك الآن تشغيل خطوة HTTPX/WebSocket باستخدام ملفات الجلسة المحفوظة.")

        except LoginError as exc:
            print(f"\n❌ {exc}")
            raise
        finally:
            # The session is already saved to disk. Keeping the browser open is not
            # required for the next HTTPX/WebSocket step.
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
