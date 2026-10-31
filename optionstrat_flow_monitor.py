from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
import sqlite3
import httpx
from websockets.asyncio.client import connect


# =========================
# الإعدادات الأساسية
# =========================
BASE_URL = "https://optionstrat.com"
FLOW_API_URL = f"{BASE_URL}/api/flow"
FLOW_WS_URL = "wss://optionstrat.com/api/ws/flow/"
API_VERSION = "1.9"
SOURCE = "0"

# الموقع يعرض وقت العقد حسب توقيت المتصفح.
# بما أن الاستخدام في السعودية، نحوله إلى توقيت الرياض (UTC+3).
FLOW_DISPLAY_TIMEZONE = timezone(timedelta(hours=3), name="Asia/Riyadh")

BASE_DIR = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "session"
COOKIES_FILE = SESSION_DIR / "cookies.json"

DATA_DIR = BASE_DIR / "data"

# نستخدم نفس قاعدة البيانات الموجودة، ولا نحذفها أو ننشئ قاعدة أخرى.
DB_FILE = DATA_DIR / "flow.db"

FAILED_RESPONSE_FILE = DATA_DIR / "last_failed_flow_response.bin"

# أنواع رسائل OptionStrat كما ظهرت في JavaScript الخاص بالموقع.
MSG_NEW_OPTIONS_FLOW = 0
MSG_QUOTE = 1
MSG_SUBSCRIBE_TO_SYMBOL = 2
MSG_PING = 3
MSG_PONG = 4
MSG_NEW_NEWS_FLOW = 5
MSG_NEW_GOVT_FLOW = 6
MSG_NEW_INSIDER_FLOW = 7

# تعريفات استراتيجيات تتكون من Calls فقط أو Puts فقط.
# الهدف هنا هو استخراج Call/Put بدون الاعتماد على Buy/Sell أو To Open.
CALL_DEFINITION_IDS = {
    1,   # Long Call
    3,   # Short Call
    7,   # Bull Call Spread
    8,   # Bear Call Spread
    11,  # Calendar Call Spread
    12,  # Diagonal Call Spread
    16,  # Long Call Butterfly
    20,  # Short Call Butterfly
    22,  # Bull Call Ladder
    23,  # Bear Call Ladder
    28,  # Call Ratio Backspread
    30,  # Long Call Condor
    32,  # Short Call Condor
    40,  # Call Broken Wing
    42,  # Inverse Call Broken Wing
    47,  # Call Ratio Spread
    58,  # Reverse Calendar Call Spread
    60,  # Reverse Diagonal Call Spread
    101, # Long Calls
    103, # Short Calls
}

PUT_DEFINITION_IDS = {
    2,   # Long Put
    4,   # Short Put
    9,   # Bull Put Spread
    10,  # Bear Put Spread
    17,  # Long Put Butterfly
    21,  # Short Put Butterfly
    24,  # Bull Put Ladder
    25,  # Bear Put Ladder
    26,  # Calendar Put Spread
    27,  # Diagonal Put Spread
    29,  # Put Ratio Backspread
    31,  # Long Put Condor
    33,  # Short Put Condor
    41,  # Put Broken Wing
    43,  # Inverse Put Broken Wing
    48,  # Put Ratio Spread
    59,  # Reverse Calendar Put Spread
    61,  # Reverse Diagonal Put Spread
    102, # Long Puts
    104, # Short Puts
}

# أسماء التعريفات كما يستخدمها OptionStrat لبناء نص عمود Strategy.
# الرقم 56 يظهر في كود الموقع لتعريفين؛ آخر تعريف داخل الخريطة هو Jade Lizard.
DEFINITION_NAMES: dict[int, str] = {
    0: "Custom",
    1: "Long Call",
    2: "Long Put",
    3: "Short Call",
    4: "Short Put",
    5: "Covered Call",
    6: "Protective Put",
    7: "Bull Call Spread",
    8: "Bear Call Spread",
    9: "Bull Put Spread",
    10: "Bear Put Spread",
    11: "Calendar Call Spread",
    12: "Diagonal Call Spread",
    13: "Collar",
    14: "Straddle",
    15: "Strangle",
    16: "Long Call Butterfly",
    17: "Long Put Butterfly",
    18: "Iron Butterfly",
    19: "Iron Condor",
    20: "Short Call Butterfly",
    21: "Short Put Butterfly",
    22: "Bull Call Ladder",
    23: "Bear Call Ladder",
    24: "Bull Put Ladder",
    25: "Bear Put Ladder",
    26: "Calendar Put Spread",
    27: "Diagonal Put Spread",
    28: "Call Ratio Backspread",
    29: "Put Ratio Backspread",
    30: "Long Call Condor",
    31: "Long Put Condor",
    32: "Short Call Condor",
    33: "Short Put Condor",
    34: "Inverse Iron Butterfly",
    35: "Inverse Iron Condor",
    36: "Covered Short Straddle",
    37: "Covered Short Strangle",
    38: "Short Straddle",
    39: "Short Strangle",
    40: "Call Broken Wing",
    41: "Put Broken Wing",
    42: "Inverse Call Broken Wing",
    43: "Inverse Put Broken Wing",
    44: "Double Diagonal",
    45: "Strap",
    46: "Strip",
    47: "Call Ratio Spread",
    48: "Put Ratio Spread",
    49: "Guts",
    50: "Short Guts",
    51: "Long Synthetic Future",
    52: "Short Synthetic Future",
    53: "Synthetic Put",
    54: "Long Combo",
    55: "Short Combo",
    56: "Jade Lizard",
    57: "Reverse Jade Lizard",
    58: "Reverse Calendar Call Spread",
    59: "Reverse Calendar Put Spread",
    60: "Reverse Diagonal Call Spread",
    61: "Reverse Diagonal Put Spread",
    101: "Long Calls",
    102: "Long Puts",
    103: "Short Calls",
    104: "Short Puts",
    105: "Long Stock",
    106: "Short Stock",
    107: "Empty",
    108: "Double Calendar",
}

# استراتيجيات تحتوي Calls وPuts معًا أو لا يصح اختصارها إلى نوع واحد.
MIXED_DEFINITION_IDS = {
    13, 14, 15, 18, 19, 34, 35, 36, 37, 38, 39,
    44, 45, 46, 49, 50, 51, 52, 54, 55, 56, 57, 108,
}

STOCK_DEFINITION_IDS = {105, 106}


class SessionExpiredError(RuntimeError):
    """الجلسة المحفوظة غير صالحة أو انتهت."""


class FlowDecodeError(RuntimeError):
    """تعذر فك بيانات OptionStrat."""


@dataclass
class Cursors:
    last_item: str | None = None
    last_news: str | None = None
    last_govt: str | None = None
    last_insider: str | None = None

    def to_query_params(self) -> dict[str, str]:
        params: dict[str, str] = {
            "version": API_VERSION,
            "source": SOURCE,
        }
        if self.last_item:
            params["lastItem"] = self.last_item
        if self.last_news:
            params["lastNews"] = self.last_news
        if self.last_govt:
            params["lastGovt"] = self.last_govt
        if self.last_insider:
            params["lastInsider"] = self.last_insider
        return params


# =========================
# قراءة الكوكيز المحفوظة
# =========================
def load_playwright_cookies() -> list[dict[str, Any]]:
    if not COOKIES_FILE.exists():
        raise FileNotFoundError(
            f"لم أجد ملف الكوكيز:\n{COOKIES_FILE}\n"
            "شغّل optionstrat_login.py أولاً."
        )

    try:
        data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"تعذر قراءة ملف الكوكيز: {COOKIES_FILE}") from exc

    if not isinstance(data, list) or not data:
        raise RuntimeError("ملف cookies.json فارغ أو صيغته غير صحيحة.")

    return [cookie for cookie in data if isinstance(cookie, dict)]


def cookie_is_for_optionstrat(cookie: dict[str, Any]) -> bool:
    domain = str(cookie.get("domain", "")).lower().lstrip(".")
    return not domain or domain == "optionstrat.com" or domain.endswith(".optionstrat.com")


def make_httpx_cookies(cookies: list[dict[str, Any]]) -> httpx.Cookies:
    jar = httpx.Cookies()

    for cookie in cookies:
        name = str(cookie.get("name", "")).strip()
        value = str(cookie.get("value", ""))
        if not name or not cookie_is_for_optionstrat(cookie):
            continue

        domain = str(cookie.get("domain") or "optionstrat.com")
        path = str(cookie.get("path") or "/")
        jar.set(name, value, domain=domain, path=path)

    return jar


def make_cookie_header(cookies: list[dict[str, Any]]) -> str:
    parts: list[str] = []

    for cookie in cookies:
        name = str(cookie.get("name", "")).strip()
        value = str(cookie.get("value", ""))
        if name and cookie_is_for_optionstrat(cookie):
            parts.append(f"{name}={value}")

    if not parts:
        raise RuntimeError("لم أجد أي Cookies تخص optionstrat.com داخل cookies.json.")

    return "; ".join(parts)


# =========================
# فك حماية OptionStrat
# =========================
def decrypt_protected_array(raw: bytes) -> bytes:
    """
    نفس خوارزمية decryptArray الموجودة في JavaScript الخاص بالموقع:
    - أول بايت: موضع التصحيح بعد فك الضغط.
    - ثاني بايت: قيمة XOR.
    - بقية البيانات: XOR ثم Raw DEFLATE ثم تصحيح بايت واحد.
    """
    if len(raw) < 3:
        raise FlowDecodeError("الرسالة المحمية أقصر من المتوقع.")

    correction_index = raw[0]
    xor_value = raw[1]

    if xor_value == 0:
        raise FlowDecodeError("قيمة XOR غير صالحة: 0")

    compressed = bytearray(raw[2:])

    for index in range(len(compressed)):
        compressed[index] ^= index % xor_value

    try:
        decoded = bytearray(
            zlib.decompress(bytes(compressed), -zlib.MAX_WBITS)
        )
    except zlib.error as exc:
        raise FlowDecodeError("فشل فك Raw DEFLATE لبيانات OptionStrat.") from exc

    if correction_index >= len(decoded):
        raise FlowDecodeError(
            f"موضع التصحيح {correction_index} خارج حجم البيانات {len(decoded)}."
        )

    decoded[correction_index] ^= xor_value
    return bytes(decoded)


def decode_api_response(response: httpx.Response) -> dict[str, Any]:
    try:
        if response.headers.get("x-protect") == "1":
            decoded = decrypt_protected_array(response.content)
            data = json.loads(decoded.decode("utf-8"))
        else:
            data = response.json()
    except Exception as exc:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        FAILED_RESPONSE_FILE.write_bytes(response.content)
        raise FlowDecodeError(
            "تعذر فك استجابة /api/flow. "
            f"تم حفظ الرد الخام هنا: {FAILED_RESPONSE_FILE}"
        ) from exc

    if not isinstance(data, dict):
        raise FlowDecodeError("استجابة /api/flow بعد فكها ليست JSON Object.")

    return data


# =========================
# تحويل كل عناصر الـ Flow إلى JSON مختصر بدون تجاهل الاستراتيجيات المختلطة
# =========================
def normalize_strike(value: Any) -> int | float | None:
    if isinstance(value, dict):
        for key in ("strike", "value", "price"):
            if key in value:
                return normalize_strike(value[key])
        return None

    if isinstance(value, bool) or value is None:
        return None

    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None

    if number == number.to_integral_value():
        return int(number)

    return float(number)

def normalize_number(value: Any) -> int | float | None:
    """
    يحول أي قيمة مالية صحيحة إلى int أو float.
    """
    if value is None or isinstance(value, bool):
        return None

    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None

    if not number.is_finite():
        return None

    if number == number.to_integral_value():
        return int(number)

    return float(number)
def normalize_expiration(value: Any) -> str | None:
    """يحوّل YYMMDD أو YYYYMMDD إلى YYYY-MM-DD."""
    if value is None:
        return None

    if isinstance(value, dict):
        for key in ("expiration", "date", "value", "code"):
            if key in value:
                return normalize_expiration(value[key])
        return None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        text = str(int(value))
    else:
        text = str(value).strip()

    if not text:
        return None

    # OptionStrat يستخدم عادة YYMMDD، وقد يصل أحياناً YYYYMMDD.
    if text.isdigit():
        if len(text) == 5:
            text = "0" + text

        if len(text) == 6:
            year = 2000 + int(text[0:2])
            month = int(text[2:4])
            day = int(text[4:6])
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                return None

        if len(text) == 8:
            year = int(text[0:4])
            month = int(text[4:6])
            day = int(text[6:8])
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                return None

    # احتياط لأي ISO date يظهر مستقبلاً.
    iso_match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if iso_match:
        try:
            return datetime(
                int(iso_match.group(1)),
                int(iso_match.group(2)),
                int(iso_match.group(3)),
            ).date().isoformat()
        except ValueError:
            return None

    return text


def parse_flow_datetime(value: Any) -> datetime | None:
    """
    يحول تاريخ OptionStrat إلى datetime بتوقيت الرياض.

    يدعم:
    - Unix seconds
    - Unix milliseconds
    - ISO datetime
    - القيم النصية الشائعة
    """
    if value is None:
        return None

    parsed: datetime | None = None

    if isinstance(value, datetime):
        parsed = value

    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)

        # تحويل milliseconds إلى seconds.
        if abs(numeric) > 10_000_000_000:
            numeric /= 1000.0

        try:
            parsed = datetime.fromtimestamp(
                numeric,
                tz=timezone.utc,
            )
        except (OverflowError, OSError, ValueError):
            return None

    else:
        text = str(value).strip()

        if not text:
            return None

        # Timestamp رقمي داخل نص.
        if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
            try:
                numeric = float(text)

                if abs(numeric) > 10_000_000_000:
                    numeric /= 1000.0

                parsed = datetime.fromtimestamp(
                    numeric,
                    tz=timezone.utc,
                )
            except (OverflowError, OSError, ValueError):
                return None

        else:
            iso_text = text

            if iso_text.endswith("Z"):
                iso_text = iso_text[:-1] + "+00:00"

            try:
                parsed = datetime.fromisoformat(iso_text)

            except ValueError:
                formats = (
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S.%f",
                )

                for fmt in formats:
                    try:
                        parsed = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue

    if parsed is None:
        return None

    # بيانات الخادم غير المزودة بتوقيت نعاملها باعتبارها UTC.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(FLOW_DISPLAY_TIMEZONE)


def normalize_flow_date(value: Any) -> str | None:
    """
    تاريخ صف الـOptions بعد التحويل إلى توقيت الرياض.
    مثال:
    2026-07-13
    """
    parsed = parse_flow_datetime(value)

    if parsed is None:
        return None

    return parsed.date().isoformat()


def normalize_flow_time(value: Any) -> str | None:
    """
    وقت صف الـOptions بعد التحويل إلى توقيت الرياض.
    مثال:
    10:15 am
    """
    parsed = parse_flow_datetime(value)

    if parsed is None:
        return None

    hour_12 = parsed.hour % 12 or 12
    am_pm = "am" if parsed.hour < 12 else "pm"

    return f"{hour_12}:{parsed.minute:02d} {am_pm}"


def normalize_report_datetime(value: Any) -> str | None:
    """
    تاريخ ووقت Insider داخل عمود واحد.

    مثال:
    2026-07-13 10:15 am
    """
    parsed = parse_flow_datetime(value)

    if parsed is None:
        return None

    hour_12 = parsed.hour % 12 or 12
    am_pm = "am" if parsed.hour < 12 else "pm"

    return (
        f"{parsed.date().isoformat()} "
        f"{hour_12}:{parsed.minute:02d} {am_pm}"
    )


def get_definition_id(item: dict[str, Any]) -> int | None:
    definition = item.get("definition")

    if isinstance(definition, dict):
        definition = definition.get("id")

    try:
        return int(definition)
    except (TypeError, ValueError):
        return None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value)
    # إزالة zero-width characters وتوحيد المسافات بدون تغيير المعنى.
    text = text.replace("​", "").replace("‌", "").replace("‍", "")
    text = text.replace("﻿", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def format_number(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def normalized_unique_values(values: Any, normalizer: Any) -> list[Any]:
    if not isinstance(values, list):
        values = [values]

    result: list[Any] = []
    seen: set[str] = set()

    for value in values:
        normalized = normalizer(value)
        if normalized is None:
            continue
        key = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        result.append(normalized)

    return result


def collapse_single(values: list[Any]) -> Any:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return values


def get_definition_name(item: dict[str, Any]) -> str | None:
    definition = item.get("definition")

    if isinstance(definition, dict):
        for key in ("name", "title", "label"):
            name = clean_text(definition.get(key))
            if name:
                return name

    for key in ("definitionName", "strategyName"):
        name = clean_text(item.get(key))
        if name:
            return name

    definition_id = get_definition_id(item)
    if definition_id is not None:
        return DEFINITION_NAMES.get(definition_id, f"Definition {definition_id}")

    return None


def infer_option_type(item: dict[str, Any], definition_name: str | None = None) -> str:
    # أولاً: أي حقل صريح قد يصل من الخادم.
    for key in ("option_type", "optionType", "putCall", "callPut", "right"):
        raw = item.get(key)
        if isinstance(raw, str):
            value = raw.strip().lower()
            if value in {"c", "call", "calls"}:
                return "call"
            if value in {"p", "put", "puts"}:
                return "put"
            if value in {"mixed", "multi", "both", "call/put", "put/call"}:
                return "mixed"

    definition_id = get_definition_id(item)
    if definition_id in STOCK_DEFINITION_IDS:
        return "stock"
    if definition_id in MIXED_DEFINITION_IDS:
        return "mixed"
    if definition_id in CALL_DEFINITION_IDS or definition_id == 5:
        return "call"
    if definition_id in PUT_DEFINITION_IDS or definition_id == 6:
        return "put"

    # الاستنتاج من اسم الاستراتيجية إذا ظهر تعريف جديد مستقبلًا.
    name = definition_name or get_definition_name(item)
    if name:
        lowered = name.lower()
        has_call = bool(re.search(r"\bcall(?:s)?\b", lowered))
        has_put = bool(re.search(r"\bput(?:s)?\b", lowered))
        if has_call and has_put:
            return "mixed"
        if has_call:
            return "call"
        if has_put:
            return "put"
        if "stock" in lowered or "equity" in lowered:
            return "stock"

    # لا نحذف العنصر مهما كانت الاستراتيجية.
    return "unknown"


def build_strategy_text(
    item: dict[str, Any],
    strikes: list[int | float],
    definition_name: str | None,
) -> str | None:
    # إذا أرسل الخادم النص جاهزًا نحتفظ به بعد تنظيف بسيط فقط.
    for key in ("strategy", "formattedStrategy", "formattedName"):
        raw = item.get(key)
        if isinstance(raw, str) and clean_text(raw):
            strategy = clean_text(raw)
            break
    else:
        strategy = None

    if strategy is None:
        name = definition_name or "Unknown Strategy"
        prefix = ""

        # نفس منطق getFormattedName داخل موقع OptionStrat.
        if name.startswith("Short "):
            prefix = "Sell "
            name = name[6:]
        elif name.startswith("Long "):
            prefix = "Buy "
            name = name[5:]

        strikes_text = "/".join(format_number(value) for value in strikes)
        parts = [part for part in (prefix.strip(), strikes_text, name) if part]
        strategy = " ".join(parts)

    if item.get("opening") and strategy and not re.search(r"\bTo Open\b$", strategy, re.I):
        strategy += " To Open"

    return clean_text(strategy)


def extract_compact_record(item: Any) -> dict[str, Any]:
    """
    يحفظ بيانات Options الحالية والجديدة.

    الحقول:
    - symbol
    - strategy
    - strike
    - option_type
    - expiration
    - flow_date
    - time
    - premium
    """
    if not isinstance(item, dict):
        return {
            "symbol": None,
            "strategy": clean_text(item),
            "strike": None,
            "option_type": "unknown",
            "expiration": None,
            "flow_date": None,
            "time": None,
            "premium": None,
        }

    symbol_text = clean_text(
        item.get("symbol")
        or item.get("underlying")
    )

    symbol = (
        symbol_text.upper()
        if symbol_text
        else None
    )

    strike_source = item.get("strikes")

    if (
        not isinstance(strike_source, list)
        or not strike_source
    ):
        strike_source = item.get("strike")

    strikes = normalized_unique_values(
        strike_source,
        normalize_strike,
    )

    expiration_source = item.get("expirations")

    if (
        not isinstance(expiration_source, list)
        or not expiration_source
    ):
        expiration_source = item.get("expiration")

    expirations = normalized_unique_values(
        expiration_source,
        normalize_expiration,
    )

    definition_name = get_definition_name(item)

    strategy = build_strategy_text(
        item,
        strikes,
        definition_name,
    )

    option_type = infer_option_type(
        item,
        definition_name,
    )

    source_datetime = item.get("date")

    return {
        "symbol": symbol,
        "strategy": strategy,
        "strike": collapse_single(strikes),
        "option_type": option_type,
        "expiration": collapse_single(expirations),

        # تاريخ العقد الحقيقي.
        "flow_date": normalize_flow_date(
            source_datetime
        ),

        # وقت العقد الحقيقي.
        "time": normalize_flow_time(
            source_datetime
        ),

        # Premium الخام كما يرسله OptionStrat.
        "premium": normalize_number(
            item.get("premium")
        ),
    }

def insider_identity(item: Any) -> str:
    """
    معرف Insider الأساسي هو id.
    """
    if isinstance(item, dict):
        insider_id = item.get("id")

        if (
            insider_id is not None
            and str(insider_id).strip()
        ):
            return f"id:{insider_id}"

    canonical = json.dumps(
        item,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()

    return f"sha256:{digest}"


def get_insider_member(item: dict[str, Any]) -> str | None:
    insider = item.get("insider")

    if isinstance(insider, dict):
        name = clean_text(
            insider.get("name")
            or insider.get("fullName")
        )

        if name:
            return name

    member = item.get("member")

    if isinstance(member, dict):
        return clean_text(
            member.get("name")
            or member.get("fullName")
        )

    return clean_text(
        item.get("memberName")
        or item.get("insiderName")
    )


def build_insider_trade(item: dict[str, Any]) -> str | None:
    """
    يبني Trade بنفس منطق الموقع تقريبًا:

    Buy TSLA
    Sell TSLA
    Acquire TSLA
    Dispose TSLA
    """
    ready_trade = clean_text(
        item.get("trade")
        or item.get("formattedTrade")
    )

    if ready_trade:
        return ready_trade

    symbol = clean_text(item.get("symbol"))

    if symbol:
        symbol = symbol.upper()

    price = normalize_number(
        item.get("price")
    )

    shares = normalize_number(
        item.get("shares")
    )

    if shares is None:
        action = "Unknown"

    elif price == 0:
        action = (
            "Acquire"
            if shares >= 0
            else "Dispose"
        )

    else:
        action = (
            "Buy"
            if shares >= 0
            else "Sell"
        )

    security_title = clean_text(
        item.get("securityTitle")
    )

    # الموقع لا يعرض هذه النصوص لأنها عادية.
    if security_title in {
        "Common Stock",
        "Ordinary Shares",
    }:
        security_title = None

    parts = [
        value
        for value in (
            action,
            symbol,
            security_title,
        )
        if value
    ]

    return " ".join(parts) or None


def calculate_insider_premium(
    item: dict[str, Any],
) -> int | float | None:
    """
    Premium الخاص بالـInsider:

    abs(price × shares)
    """
    price = normalize_number(
        item.get("price")
    )

    shares = normalize_number(
        item.get("shares")
    )

    if price is None or shares is None:
        return None

    return normalize_number(
        abs(
            Decimal(str(price))
            * Decimal(str(shares))
        )
    )


def extract_insider_record(
    item: Any,
) -> dict[str, Any]:
    """
    أعمدة جدول Insider:

    Report Date
    Trade
    Member
    Premium
    """
    if not isinstance(item, dict):
        return {
            "report_date": None,
            "trade": clean_text(item),
            "member": None,
            "premium": None,
        }

    # الموقع يستخدم filingDate كتاريخ الصف.
    report_date_source = (
        item.get("filingDate")
        or item.get("reportDate")
        or item.get("date")
    )

    return {
        # التاريخ والوقت داخل نفس العمود.
        "report_date": normalize_report_datetime(
            report_date_source
        ),

        "trade": build_insider_trade(item),

        "member": get_insider_member(item),

        "premium": calculate_insider_premium(
            item
        ),
    }

def item_identity(item: Any) -> str:
    if isinstance(item, dict):
        code = item.get("code")
        if code is not None and str(code).strip():
            return f"code:{code}"

    canonical = json.dumps(item, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def get_cursor(items: Any, key: str) -> str | None:
    if not isinstance(items, list) or not items:
        return None

    first = items[0]
    if not isinstance(first, dict):
        return None

    value = first.get(key)
    return str(value) if value is not None else None


# =========================
# HTTPX: جلب الـ Flow الحالي
# =========================
async def fetch_current_flow(cookies: list[dict[str, Any]]) -> dict[str, Any]:
    headers = {
        "Accept": "*/*",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": f"{BASE_URL}/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/150.0.0.0 Safari/537.36"
        ),
        "x-version": API_VERSION,
        "x-source": SOURCE,
    }

    timeout = httpx.Timeout(30.0, connect=20.0)

    async with httpx.AsyncClient(
        cookies=make_httpx_cookies(cookies),
        headers=headers,
        timeout=timeout,
        follow_redirects=False,
        http2=True,
    ) as client:
        response = await client.get(FLOW_API_URL)

    if response.status_code in {301, 302, 303, 307, 308}:
        location = response.headers.get("location", "")
        raise SessionExpiredError(
            f"أعاد الموقع توجيه الطلب إلى {location or 'صفحة أخرى'}؛ شغّل تسجيل الدخول من جديد."
        )

    if response.status_code in {401, 403}:
        raise SessionExpiredError(
            f"الجلسة غير صالحة أو انتهت. HTTP {response.status_code}. "
            "شغّل optionstrat_login.py من جديد."
        )

    response.raise_for_status()
    return decode_api_response(response)


# =========================
# WebSocket: مراقبة الـ Live Flow
# =========================
def read_7bit_encoded_int(data: bytes, start: int) -> tuple[int, int]:
    value = 0
    shift = 0
    index = start

    while True:
        if index >= len(data):
            raise FlowDecodeError("انتهت الرسالة أثناء قراءة طول JSON.")

        byte = data[index]
        index += 1
        value |= (byte & 0x7F) << shift

        if (byte & 0x80) == 0:
            return value, index

        shift += 7
        if shift >= 35:
            raise FlowDecodeError("صيغة 7-bit integer غير صالحة.")


def decode_websocket_message(raw: bytes) -> tuple[int, Any | None]:
    decoded = decrypt_protected_array(raw)

    if not decoded:
        raise FlowDecodeError("رسالة WebSocket فارغة بعد فكها.")

    message_type = decoded[0]

    # Ping/Pong لا يحملان JSON.
    if message_type in {MSG_PING, MSG_PONG}:
        return message_type, None

    # نهتم فقط برسائل الـ Flow الأربع؛ أي نوع آخر يترك بدون محاولة فك String.
    if message_type not in {
        MSG_NEW_OPTIONS_FLOW,
        MSG_NEW_NEWS_FLOW,
        MSG_NEW_GOVT_FLOW,
        MSG_NEW_INSIDER_FLOW,
    }:
        return message_type, None

    text_length, text_start = read_7bit_encoded_int(decoded, 1)
    text_end = text_start + text_length

    if text_end > len(decoded):
        raise FlowDecodeError(
            f"طول JSON المطلوب {text_length} أكبر من البيانات المتبقية."
        )

    try:
        payload = json.loads(decoded[text_start:text_end].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FlowDecodeError("تعذر قراءة JSON داخل رسالة WebSocket.") from exc

    return message_type, payload


def payload_as_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return list(payload)
    if payload is None:
        return []
    return [payload]

# =========================
# SQLite
# =========================

def get_db_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    conn = sqlite3.connect(
        DB_FILE,
        timeout=30,
        check_same_thread=False,
    )

    # يسمح للباك اند بالقراءة أثناء كتابة المراقب.
    conn.execute(
        "PRAGMA journal_mode=WAL;"
    )

    conn.execute(
        "PRAGMA synchronous=NORMAL;"
    )

    return conn


def get_table_columns(
    conn: sqlite3.Connection,
    table_name: str,
) -> set[str]:
    cursor = conn.execute(
        f"PRAGMA table_info({table_name})"
    )

    return {
        str(row[1])
        for row in cursor.fetchall()
    }


def add_column_if_missing(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    columns = get_table_columns(
        conn,
        table_name,
    )

    if column_name in columns:
        return

    conn.execute(
        f"""
        ALTER TABLE {table_name}
        ADD COLUMN {column_name}
        {column_definition}
        """
    )


def init_db(
    conn: sqlite3.Connection,
) -> None:
    """
    لا يحذف قاعدة البيانات القديمة.

    يستخدم:
    - flow_items للـOptions
    - insider_flow للـInsider
    """

    # جدول Options القديم والحالي والـLive.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS flow_items (
            identity TEXT PRIMARY KEY,
            symbol TEXT,
            strategy TEXT,
            strike TEXT,
            option_type TEXT,
            expiration TEXT,
            flow_date TEXT,
            time TEXT,
            premium REAL,
            inserted_at TEXT NOT NULL
        )
        """
    )

    # Migration للقاعدة القديمة.
    add_column_if_missing(
        conn,
        "flow_items",
        "flow_date",
        "TEXT",
    )

    add_column_if_missing(
        conn,
        "flow_items",
        "premium",
        "REAL",
    )

    # جدول Insider مستقل داخل نفس flow.db.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS insider_flow (
            identity TEXT PRIMARY KEY,
            report_date TEXT,
            trade TEXT,
            member TEXT,
            premium REAL,
            inserted_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_flow_items_symbol
        ON flow_items(symbol)
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_flow_items_flow_date
        ON flow_items(flow_date)
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_insider_flow_report_date
        ON insider_flow(report_date)
        """
    )

    conn.commit()


def load_option_identities(
    conn: sqlite3.Connection,
) -> set[str]:
    cursor = conn.execute(
        """
        SELECT identity
        FROM flow_items
        """
    )

    return {
        str(row[0])
        for row in cursor.fetchall()
    }


def load_insider_identities(
    conn: sqlite3.Connection,
) -> set[str]:
    cursor = conn.execute(
        """
        SELECT identity
        FROM insider_flow
        """
    )

    return {
        str(row[0])
        for row in cursor.fetchall()
    }


def upsert_option_records(
    conn: sqlite3.Connection,
    records: list[
        tuple[str, dict[str, Any]]
    ],
) -> None:
    """
    يحدث القديم ويضيف الجديد.

    هذا مهم حتى تملأ:
    flow_date
    premium

    للعقود الموجودة مسبقًا.
    """
    if not records:
        return

    inserted_at = datetime.now(
        timezone.utc
    ).isoformat()

    rows = []

    for identity, record in records:
        rows.append(
            (
                identity,
                record.get("symbol"),
                record.get("strategy"),

                json.dumps(
                    record.get("strike"),
                    ensure_ascii=False,
                ),

                record.get("option_type"),

                json.dumps(
                    record.get("expiration"),
                    ensure_ascii=False,
                ),

                record.get("flow_date"),
                record.get("time"),
                record.get("premium"),
                inserted_at,
            )
        )

    conn.executemany(
        """
        INSERT INTO flow_items (
            identity,
            symbol,
            strategy,
            strike,
            option_type,
            expiration,
            flow_date,
            time,
            premium,
            inserted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        ON CONFLICT(identity)
        DO UPDATE SET

            symbol =
                excluded.symbol,

            strategy =
                excluded.strategy,

            strike =
                excluded.strike,

            option_type =
                excluded.option_type,

            expiration =
                excluded.expiration,

            flow_date =
                excluded.flow_date,

            time =
                excluded.time,

            premium =
                excluded.premium
        """,
        rows,
    )

    conn.commit()


def upsert_insider_records(
    conn: sqlite3.Connection,
    records: list[
        tuple[str, dict[str, Any]]
    ],
) -> None:
    """
    يحفظ Insider الحالي والحي
    داخل insider_flow.
    """
    if not records:
        return

    inserted_at = datetime.now(
        timezone.utc
    ).isoformat()

    rows = []

    for identity, record in records:
        rows.append(
            (
                identity,
                record.get("report_date"),
                record.get("trade"),
                record.get("member"),
                record.get("premium"),
                inserted_at,
            )
        )

    conn.executemany(
        """
        INSERT INTO insider_flow (
            identity,
            report_date,
            trade,
            member,
            premium,
            inserted_at
        )
        VALUES (?, ?, ?, ?, ?, ?)

        ON CONFLICT(identity)
        DO UPDATE SET

            report_date =
                excluded.report_date,

            trade =
                excluded.trade,

            member =
                excluded.member,

            premium =
                excluded.premium
        """,
        rows,
    )

    conn.commit()


def count_database_rows(
    conn: sqlite3.Connection,
) -> tuple[int, int]:
    option_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM flow_items
        """
    ).fetchone()[0]

    insider_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM insider_flow
        """
    ).fetchone()[0]

    return (
        int(option_count),
        int(insider_count),
    )
class FlowMonitor:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.seen_items: set[str] = set()
        self.cursors = Cursors()
        self.last_received_monotonic = time.monotonic()

    async def initialize(self) -> None:
        # فتح نفس قاعدة البيانات الموجودة.
        self.conn = await asyncio.to_thread(
            get_db_connection
        )

        await asyncio.to_thread(
            init_db,
            self.conn,
        )

        self.seen_options = (
            await asyncio.to_thread(
                load_option_identities,
                self.conn,
            )
        )

        self.seen_insiders = (
            await asyncio.to_thread(
                load_insider_identities,
                self.conn,
            )
        )

        cookies = load_playwright_cookies()

        print(
            "📥 جلب Options Flow و"
            "Insider Flow بواسطة HTTPX..."
        )

        flow = await fetch_current_flow(
            cookies
        )

        history = flow.get(
            "history",
            [],
        )

        news = flow.get(
            "news",
            [],
        )

        govt = flow.get(
            "govt",
            [],
        )

        insider = flow.get(
            "insider",
            [],
        )

        self.cursors = Cursors(
            last_item=get_cursor(
                history,
                "code",
            ),

            last_news=get_cursor(
                news,
                "id",
            ),

            last_govt=get_cursor(
                govt,
                "id",
            ),

            last_insider=get_cursor(
                insider,
                "id",
            ),
        )

        option_rows: list[
            tuple[str, dict[str, Any]]
        ] = []

        if isinstance(history, list):
            for item in history:
                identity = item_identity(item)

                self.seen_options.add(
                    identity
                )

                option_rows.append(
                    (
                        identity,
                        extract_compact_record(
                            item
                        ),
                    )
                )

        insider_rows: list[
            tuple[str, dict[str, Any]]
        ] = []

        if isinstance(insider, list):
            for item in insider:
                identity = insider_identity(
                    item
                )

                self.seen_insiders.add(
                    identity
                )

                insider_rows.append(
                    (
                        identity,
                        extract_insider_record(
                            item
                        ),
                    )
                )

        # نعمل UPSERT للجميع،
        # حتى تتحدث الأعمدة الجديدة
        # للعقود القديمة.
        await asyncio.to_thread(
            upsert_option_records,
            self.conn,
            option_rows,
        )

        await asyncio.to_thread(
            upsert_insider_records,
            self.conn,
            insider_rows,
        )

        option_total, insider_total = (
            await asyncio.to_thread(
                count_database_rows,
                self.conn,
            )
        )

        print(
            "\n📦 البيانات التي أعادها "
            "/api/flow:"
        )

        print(
            f"Options: {len(option_rows)}"
        )

        print(
            f"Insider: {len(insider_rows)}"
        )

        print(
            "\n✅ تمت مزامنة Options "
            "داخل flow_items"
        )

        print(
            "✅ تمت مزامنة Insider "
            "داخل insider_flow"
        )

        print(
            f"\n📊 إجمالي flow_items: "
            f"{option_total}"
        )

        print(
            f"📊 إجمالي insider_flow: "
            f"{insider_total}"
        )

        print(
            f"\n🗄️ قاعدة البيانات:\n"
            f"{DB_FILE}"
        )


    def build_websocket_url(self) -> str:
        return f"{FLOW_WS_URL}?{urlencode(self.cursors.to_query_params())}"

    async def heartbeat(self, websocket: Any) -> None:
        while True:
            await asyncio.sleep(30)
            sent_at = time.monotonic()
            await websocket.send(bytes([MSG_PING]))

            await asyncio.sleep(10)
            if self.last_received_monotonic < sent_at:
                print("⚠️ لم يصل رد بعد Ping؛ سيتم إعادة الاتصال...")
                await websocket.close(code=1011, reason="Heartbeat timeout")
                return

    async def handle_flow_payload(
        self,
        message_type: int,
        payload: Any,
    ) -> None:

        items = payload_as_items(
            payload
        )

        if not items:
            return

        last = items[-1]

        # =====================
        # News
        # =====================
        if (
            message_type
            == MSG_NEW_NEWS_FLOW
        ):
            if (
                isinstance(last, dict)
                and last.get("id") is not None
            ):
                self.cursors.last_news = str(
                    last["id"]
                )

            return

        # =====================
        # Government
        # =====================
        if (
            message_type
            == MSG_NEW_GOVT_FLOW
        ):
            if (
                isinstance(last, dict)
                and last.get("id") is not None
            ):
                self.cursors.last_govt = str(
                    last["id"]
                )

            return

        # =====================
        # Options Live
        # =====================
        if (
            message_type
            == MSG_NEW_OPTIONS_FLOW
        ):
            if (
                isinstance(last, dict)
                and last.get("code")
                is not None
            ):
                self.cursors.last_item = str(
                    last["code"]
                )

            option_rows: list[
                tuple[
                    str,
                    dict[str, Any],
                ]
            ] = []

            for item in items:
                identity = item_identity(
                    item
                )

                if (
                    identity
                    in self.seen_options
                ):
                    continue

                self.seen_options.add(
                    identity
                )

                option_rows.append(
                    (
                        identity,
                        extract_compact_record(
                            item
                        ),
                    )
                )

            if not option_rows:
                return

            assert self.conn is not None

            await asyncio.to_thread(
                upsert_option_records,
                self.conn,
                option_rows,
            )

            for _, record in option_rows:
                print(
                    "🆕 OPTIONS | "
                    f"{record['symbol']} | "
                    f"Strategy: "
                    f"{record['strategy']} | "
                    f"Strike: "
                    f"{record['strike']} | "
                    f"Type: "
                    f"{record['option_type']} | "
                    f"Expiration: "
                    f"{record['expiration']} | "
                    f"Date: "
                    f"{record['flow_date']} | "
                    f"Time: "
                    f"{record['time']} | "
                    f"Premium: "
                    f"{record['premium']}"
                )

            return

        # =====================
        # Insider Live
        # =====================
        if (
            message_type
            == MSG_NEW_INSIDER_FLOW
        ):
            if (
                isinstance(last, dict)
                and last.get("id")
                is not None
            ):
                self.cursors.last_insider = str(
                    last["id"]
                )

            insider_rows: list[
                tuple[
                    str,
                    dict[str, Any],
                ]
            ] = []

            for item in items:
                identity = insider_identity(
                    item
                )

                if (
                    identity
                    in self.seen_insiders
                ):
                    continue

                self.seen_insiders.add(
                    identity
                )

                insider_rows.append(
                    (
                        identity,
                        extract_insider_record(
                            item
                        ),
                    )
                )

            if not insider_rows:
                return

            assert self.conn is not None

            await asyncio.to_thread(
                upsert_insider_records,
                self.conn,
                insider_rows,
            )

            for _, record in insider_rows:
                print(
                    "🆕 INSIDER | "
                    f"Report Date: "
                    f"{record['report_date']} | "
                    f"Trade: "
                    f"{record['trade']} | "
                    f"Member: "
                    f"{record['member']} | "
                    f"Premium: "
                    f"{record['premium']}"
                )

            return

    async def monitor_live(self) -> None:
        while True:
            heartbeat_task: asyncio.Task[None] | None = None

            try:
                cookies = load_playwright_cookies()
                cookie_header = make_cookie_header(cookies)
                websocket_url = self.build_websocket_url()

                print("\n🔌 الاتصال بـ Live Flow WebSocket...")

                async with connect(
                    websocket_url,
                    origin=BASE_URL,
                    subprotocols=["optionstrat"],
                    additional_headers={
                        "Cookie": cookie_header,
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                    },
                    user_agent_header=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/150.0.0.0 Safari/537.36"
                    ),
                    open_timeout=20,
                    close_timeout=5,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=None,
                ) as websocket:

                    print(
                        "✅ بدأ رصد Options Live "
                        "وInsider Live من نفس WebSocket"
                    )

                    print(
                        "   Type 0 → flow_items"
                    )

                    print(
                        "   Type 7 → insider_flow"
                    )
                    print("⏹️ للإيقاف اضغط Ctrl + C")

                    self.last_received_monotonic = time.monotonic()

                    # الموقع يرسل Ping مباشرة بعد فتح الاتصال.
                    await websocket.send(bytes([MSG_PING]))
                    heartbeat_task = asyncio.create_task(self.heartbeat(websocket))

                    async for message in websocket:
                        self.last_received_monotonic = time.monotonic()

                        if isinstance(message, str):
                            # المتوقع Binary؛ نتجاهل أي نص غير متوقع.
                            continue

                        try:
                            message_type, payload = decode_websocket_message(bytes(message))
                        except FlowDecodeError as exc:
                            print(f"⚠️ تعذر فك رسالة WebSocket: {exc}")
                            continue

                        if message_type == MSG_PING:
                            await websocket.send(bytes([MSG_PONG]))
                            continue

                        if message_type == MSG_PONG:
                            continue

                        await self.handle_flow_payload(message_type, payload)

            except asyncio.CancelledError:
                raise
            except SessionExpiredError:
                raise
            except Exception as exc:
                delay = random.uniform(2.0, 5.0)
                print(f"⚠️ انقطع اتصال الـ WebSocket: {exc}")
                print(f"🔄 إعادة المحاولة بعد {delay:.1f} ثانية...")
                await asyncio.sleep(delay)
            finally:
                if heartbeat_task is not None:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass


async def main() -> None:
    monitor = FlowMonitor()
    await monitor.initialize()
    await monitor.monitor_live()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SessionExpiredError as exc:
        print(f"\n❌ {exc}")
    except FileNotFoundError as exc:
        print(f"\n❌ {exc}")
    except httpx.HTTPStatusError as exc:
        print(
            "\n❌ فشل طلب /api/flow: "
            f"HTTP {exc.response.status_code} - {exc.response.text[:300]}"
        )
    except KeyboardInterrupt:
        print("\n🛑 تم إيقاف المراقبة.")
    except Exception as exc:
        print(f"\n❌ خطأ غير متوقع: {type(exc).__name__}: {exc}")
