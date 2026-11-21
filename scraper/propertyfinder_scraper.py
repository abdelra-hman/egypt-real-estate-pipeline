import csv
import glob
import json
import logging
import math
import os
import random
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests

logger = logging.getLogger("PropertyFinderScraper")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _ch = logging.StreamHandler(sys.stdout)
    _ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(_ch)

BASE_URL = "https://www.propertyfinder.eg"
DEFAULT_LOCATIONS = [
    "cairo", "giza", "alexandria", "al-daqahlya", "red-sea", "north-coast",
    "qalyubia", "al-gharbeya", "suez", "demyat", "sharqia", "port-saeed",
    "south-sainai", "el-esmailia", "al-menya", "kafr-el-sheikh", "al-behera",
]

URL_TEMPLATES = {
    "buy": "/en/buy/{city}/properties-for-sale.html",
    "rent": "/en/rent/{city}/properties-for-rent.html",
}
LISTING_TYPE = "property"

HEADERS = {
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.propertyfinder.eg/en/",
}

DEFAULT_DELAY = 3.0
MAX_RETRIES = 4
MAX_CONSECUTIVE_FAILURES = 5
MAX_RETRY_AFTER_SECONDS = 300
BLOCK_COOLDOWN_SECONDS = 30
FLUSH_BATCH = 10

CSV_COLUMNS = [
    "listing_id", "internal_id", "category", "listing_type", "detail_url",
    "property_type", "offering_type", "completion_status", "title",
    "price_egp", "price_period", "price_currency",
    "location_full", "city", "town", "district", "subdistrict", "lat", "lon",
    "bedrooms", "bathrooms", "area_value", "area_unit", "furnished",
    "listing_level", "is_premium", "is_verified", "is_featured",
    "is_new_construction", "is_direct_from_dev", "is_exclusive",
    "listed_date", "images_count", "has_view_360", "video_url",
    "reference", "rera", "description", "amenities", "payment_method",
    "agent_id", "agent_name", "agent_email", "agent_is_super", "agent_languages",
    "broker_id", "broker_name", "broker_email", "broker_phone",
    "contact_phone", "contact_whatsapp", "contact_email", "scraped_at",
]

HISTORY_COLUMNS = [
    "listing_id", "category", "field_changed", "old_value",
    "new_value", "price_currency", "detected_at",
]
TRACKED_FIELDS = ("price_egp", "area_value")
AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


class Stats:
    def __init__(self):
        self.discovered = 0
        self.extracted = 0
        self.unchanged = 0
        self.updated = 0
        self.failed = 0
        self.http_errors = 0
        self.start_time = time.time()

    def report(self, csv_path, history_rows=0):
        duration = round(time.time() - self.start_time, 1)
        logger.info("=" * 60)
        logger.info("  SCRAPING EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Total listings discovered : {self.discovered:,}")
        logger.info(f"  New listings extracted    : {self.extracted:,}")
        logger.info(f"  Unchanged (already saved) : {self.unchanged:,}")
        logger.info(f"  Updated (price/area)      : {self.updated:,}")
        logger.info(f"  Failed / invalid          : {self.failed:,}")
        logger.info(f"  HTTP errors encountered   : {self.http_errors:,}")
        logger.info(f"  Execution duration        : {duration} seconds")
        logger.info(f"  History rows recorded     : {history_rows:,}")
        logger.info(f"  CSV file path             : {csv_path}")
        logger.info("=" * 60)


def normalize_numeric(value, allow_negative=False):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (int, float)):
        return value if (allow_negative or value >= 0) else None
    if isinstance(value, str):
        cleaned = re.sub(r"[,٬\s]", "", value.translate(AR_DIGITS).strip())
        if not cleaned:
            return None
        try:
            val = float(cleaned) if "." in cleaned else int(cleaned)
        except ValueError:
            return None
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val if (allow_negative or val >= 0) else None
    return None


def num_str(value):
    if value is None:
        return ""
    s = str(value).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none"):
        return ""
    try:
        f = float(s)
    except ValueError:
        return s
    if math.isnan(f) or math.isinf(f):
        return ""
    return str(int(f)) if f.is_integer() else str(f)


def cell(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return num_str(value)
    return str(value)


def dig(obj, *path):
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def first(*values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, (list, dict)) and not v:
            continue
        return v
    return None


def text(value):
    if value is None:
        return None
    s = " ".join(str(value).split())
    return s or None


def extract_label(value):
    if isinstance(value, dict):
        value = first(value.get("name"), value.get("title"), value.get("label"), value.get("value"))
    return text(value)


def to_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return None
        return s in ("true", "1", "yes", "y") or s.startswith("http")
    return None


def join_names(value, sep=" | "):
    if value is None:
        return None
    if isinstance(value, str):
        return text(value)
    if isinstance(value, dict):
        return extract_label(value)
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            if isinstance(item, dict):
                item = first(item.get("name"), item.get("title"), item.get("label"), item.get("value"))
            t = text(item)
            if t and t not in out:
                out.append(t)
        return sep.join(out) if out else None
    return text(value)


def phone_str(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return str(int(value))
    s = str(value).strip()
    if s.endswith(".0") and s[:-2].lstrip("+").isdigit():
        s = s[:-2]
    return s or None


def get_location_by_type(location_tree, loc_type):
    if not isinstance(location_tree, list):
        return None
    for loc in location_tree:
        if isinstance(loc, dict) and str(loc.get("type") or "").upper() == loc_type:
            name = loc.get("name")
            return str(name) if name is not None else None
    return None


def contact_value(prop_data, kind):
    options = prop_data.get("contact_options")
    if isinstance(options, list):
        for opt in options:
            if isinstance(opt, dict) and str(opt.get("type") or "").strip().lower() == kind:
                v = first(opt.get("value"), opt.get("number"), opt.get("link"))
                if isinstance(v, str) and v.lower().startswith("mailto:"):
                    v = v[7:].strip() or None
                if v is not None:
                    return v
    return None


def split_ids(prop_data):
    ids = []
    for key in ("listing_id", "id", "internal_id"):
        v = prop_data.get(key)
        if v is None or isinstance(v, bool):
            continue
        s = str(v).strip()
        if s.endswith(".0") and s[:-2].isdigit():
            s = s[:-2]
        if s and s not in ids:
            ids.append(s)
    alnum = next((i for i in ids if not i.isdigit()), None)
    digits = next((i for i in ids if i.isdigit()), None)
    if alnum:
        return alnum, digits
    return digits, None


def validate_and_normalize_url(url_raw):
    if not url_raw or not isinstance(url_raw, str):
        return None
    url_raw = url_raw.strip()
    if url_raw.startswith("/") and not url_raw.startswith("//"):
        return BASE_URL + url_raw
    try:
        parsed = urlparse(url_raw)
        if parsed.scheme != "https":
            return None
        if parsed.hostname not in ("www.propertyfinder.eg", "propertyfinder.eg"):
            return None
        return url_raw
    except Exception:
        return None


def now_ts():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="microseconds")


def parse_listing(prop_data, category):
    if not isinstance(prop_data, dict):
        return None

    listing_id, internal_id = split_ids(prop_data)
    if not listing_id:
        return None

    share_url = prop_data.get("share_url")
    details_path = prop_data.get("details_path")
    raw_url = share_url if isinstance(share_url, str) and share_url else details_path
    detail_url = validate_and_normalize_url(raw_url)
    if not detail_url:
        return None

    price_raw = prop_data.get("price")
    price_info = price_raw if isinstance(price_raw, dict) else {}
    price_val = price_info.get("value") if price_info else price_raw
    size_raw = prop_data.get("size")
    size_info = size_raw if isinstance(size_raw, dict) else {}
    size_val = size_info.get("value") if size_info else size_raw

    location_tree = prop_data.get("location_tree")
    loc = prop_data.get("location")
    loc = loc if isinstance(loc, dict) else {}

    location_full = text(first(loc.get("full_name"), loc.get("path_name")))
    if not location_full:
        names = [loc.get("name")]
        if isinstance(location_tree, list):
            names += [t.get("name") for t in reversed(location_tree) if isinstance(t, dict)]
        seen = []
        for n in names:
            n = text(n)
            if n and n not in seen:
                seen.append(n)
        location_full = ", ".join(seen) or None

    coords = loc.get("coordinates")
    lat_val = lon_val = None
    if isinstance(coords, dict):
        lat_val = normalize_numeric(coords.get("lat"), allow_negative=True)
        lon_val = normalize_numeric(first(coords.get("lon"), coords.get("lng")), allow_negative=True)
    lat = lat_val if lat_val is not None and -90 <= lat_val <= 90 else None
    lon = lon_val if lon_val is not None and -180 <= lon_val <= 180 else None

    level = extract_label(first(prop_data.get("listing_level"), prop_data.get("tier")))
    level_l = (level or "").lower()
    is_premium = to_bool(first(prop_data.get("is_premium"), (level_l == "premium") if level else None))
    is_featured = to_bool(first(prop_data.get("is_featured"), (level_l == "featured") if level else None))

    images = prop_data.get("images")
    images_count = normalize_numeric(first(
        prop_data.get("images_count"), prop_data.get("image_count"),
        len(images) if isinstance(images, list) else None,
    ))

    videos = prop_data.get("videos")
    video_url = first(
        prop_data.get("video_url"), dig(prop_data, "video", "url"),
        (videos[0].get("url") if isinstance(videos, list) and videos and isinstance(videos[0], dict)
         else (videos[0] if isinstance(videos, list) and videos and isinstance(videos[0], str) else None)),
    )

    agent = prop_data.get("agent")
    agent = agent if isinstance(agent, dict) else {}
    broker = prop_data.get("broker")
    broker = broker if isinstance(broker, dict) else {}

    contact_email = first(contact_value(prop_data, "email"), agent.get("email"))
    agent_email = first(agent.get("email"), contact_email)
    contact_phone = first(contact_value(prop_data, "phone"), agent.get("phone"), prop_data.get("phone"))
    contact_whatsapp = first(contact_value(prop_data, "whatsapp"), agent.get("whatsapp"), prop_data.get("whatsapp"))

    row = {
        "listing_id": listing_id,
        "internal_id": internal_id,
        "category": category,
        "listing_type": LISTING_TYPE,
        "detail_url": detail_url,
        "property_type": extract_label(prop_data.get("property_type")),
        "offering_type": extract_label(prop_data.get("offering_type")),
        "completion_status": extract_label(prop_data.get("completion_status")),
        "title": text(prop_data.get("title")),
        "price_egp": normalize_numeric(price_val),
        "price_period": text(price_info.get("period")),
        "price_currency": text(price_info.get("currency")),
        "location_full": location_full,
        "city": get_location_by_type(location_tree, "CITY"),
        "town": get_location_by_type(location_tree, "TOWN"),
        "district": get_location_by_type(location_tree, "DISTRICT"),
        "subdistrict": get_location_by_type(location_tree, "SUBDISTRICT"),
        "lat": lat,
        "lon": lon,
        "bedrooms": text(first(prop_data.get("bedrooms"), prop_data.get("bedrooms_value"))),
        "bathrooms": text(first(prop_data.get("bathrooms"), prop_data.get("bathrooms_value"))),
        "area_value": normalize_numeric(size_val),
        "area_unit": text(size_info.get("unit")),
        "furnished": extract_label(prop_data.get("furnished")),
        "listing_level": level,
        "is_premium": is_premium,
        "is_verified": to_bool(first(prop_data.get("is_verified"), prop_data.get("verified"))),
        "is_featured": is_featured,
        "is_new_construction": to_bool(first(prop_data.get("is_new_construction"), prop_data.get("new_construction"))),
        "is_direct_from_dev": to_bool(first(
            prop_data.get("is_direct_from_developer"), prop_data.get("is_direct_from_dev"),
            prop_data.get("direct_from_developer"))),
        "is_exclusive": to_bool(first(prop_data.get("is_exclusive"), prop_data.get("exclusive"))),
        "listed_date": text(prop_data.get("listed_date")),
        "images_count": None if images_count is None else int(images_count),
        "has_view_360": to_bool(first(
            prop_data.get("has_view_360"), prop_data.get("has_360_view"),
            prop_data.get("view_360"), prop_data.get("is_360_view"))),
        "video_url": text(video_url),
        "reference": text(prop_data.get("reference")),
        "rera": text(prop_data.get("rera")),
        "description": text(prop_data.get("description")),
        "amenities": join_names(first(prop_data.get("amenity_names"), prop_data.get("amenities"))),
        "payment_method": join_names(prop_data.get("payment_method")),
        "agent_id": normalize_numeric(agent.get("id")),
        "agent_name": text(agent.get("name")),
        "agent_email": text(agent_email),
        "agent_is_super": to_bool(first(
            agent.get("is_super_agent"), agent.get("super_agent"),
            agent.get("has_super_agent_status"), agent.get("is_super"))),
        "agent_languages": join_names(agent.get("languages")),
        "broker_id": normalize_numeric(broker.get("id")),
        "broker_name": text(broker.get("name")),
        "broker_email": text(broker.get("email")),
        "broker_phone": phone_str(broker.get("phone")),
        "contact_phone": phone_str(contact_phone),
        "contact_whatsapp": phone_str(contact_whatsapp),
        "contact_email": text(contact_email),
        "scraped_at": now_ts(),
    }
    return {c: row[c] for c in CSV_COLUMNS}


def same_value(a, b):
    a = "" if a is None else str(a).strip()
    b = "" if b is None else str(b).strip()
    if a == b:
        return True
    try:
        return float(a) == float(b)
    except ValueError:
        return False


class Store:
    def __init__(self, path, history_path, landing_dir):
        self.path = path
        self.history_path = history_path
        self.landing_dir = landing_dir
        self.rows = {}
        self.prev_rows = {}
        self.pending_history = []
        self.last_change = {}
        self.history_recorded = 0
        self.dirty = 0
        self._threshold = FLUSH_BATCH

    def load(self):
        self._ensure_history_file()
        self._load_previous_snapshot()

    def _ensure_history_file(self):
        if os.path.exists(self.history_path) and os.path.getsize(self.history_path) > 0:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.history_path)) or ".", exist_ok=True)
        with open(self.history_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL, escapechar="\\").writerow(HISTORY_COLUMNS)

    def _load_previous_snapshot(self):
        files = sorted(glob.glob(os.path.join(self.landing_dir, "pf_listings_*.csv")))
        files = [f for f in files if os.path.abspath(f) != os.path.abspath(self.path)]
        if not files:
            return
        latest_file = files[-1]
        try:
            with open(latest_file, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, escapechar="\\")
                for row in reader:
                    lid = row.get("listing_id")
                    if lid:
                        self.prev_rows[lid] = row
            logger.info(f"Loaded {len(self.prev_rows):,} previous listings from {os.path.basename(latest_file)} for history tracking.")
        except Exception as e:
            logger.warning(f"Could not load previous snapshot {latest_file}: {e}")

    def upsert(self, parsed):
        lid = parsed["listing_id"]
        new_row = {c: cell(parsed.get(c)) for c in CSV_COLUMNS}
        old_current = self.rows.get(lid)
        old_ref = old_current if old_current is not None else self.prev_rows.get(lid)

        if old_ref is None:
            self.rows[lid] = new_row
            self.dirty += 1
            return "new", []

        changes = []
        for field in TRACKED_FIELDS:
            o, n = num_str(old_ref.get(field)), num_str(parsed.get(field))
            if o and n and o != n:
                changes.append((field, o, n))

        merged = {c: (new_row[c] if new_row[c] != "" else old_ref.get(c, "")) for c in CSV_COLUMNS}
        content_changed = any(
            c != "scraped_at" and not same_value(merged[c], old_ref.get(c, ""))
            for c in CSV_COLUMNS
        )

        # Always store in current run's snapshot
        self.rows[lid] = merged
        self.dirty += 1

        if not content_changed:
            return "same", []

        for field, o, n in changes:
            if self.last_change.get((lid, field)) == (o, n):
                continue
            self.pending_history.append({
                "listing_id": lid,
                "category": merged.get("category", ""),
                "field_changed": field,
                "old_value": o,
                "new_value": n,
                "price_currency": merged.get("price_currency", ""),
                "detected_at": parsed["scraped_at"],
            })
            self.last_change[(lid, field)] = (o, n)
            self.history_recorded += 1

        return ("updated" if changes else "same"), changes

    def maybe_flush(self):
        if self.dirty >= self._threshold:
            ok = self.flush()
            self._threshold = FLUSH_BATCH if ok else self.dirty + FLUSH_BATCH

    def flush(self):
        if self.dirty == 0 and not self.pending_history:
            return True
        if self.pending_history and not self._append_history():
            return False
        if self.dirty and not self._write_current():
            return False
        return True

    def _append_history(self):
        try:
            self._ensure_history_file()
            with open(self.history_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=HISTORY_COLUMNS, quoting=csv.QUOTE_MINIMAL, escapechar="\\"
                )
                writer.writerows(self.pending_history)
            self.pending_history = []
            return True
        except Exception as e:
            logger.error(f"Failed to append history: {e}")
            return False

    def _write_current(self):
        target_dir = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(target_dir, exist_ok=True)
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", newline="", encoding="utf-8", dir=target_dir, delete=False, suffix=".tmp"
            ) as tmp_f:
                tmp_path = tmp_f.name
                writer = csv.DictWriter(
                    tmp_f, fieldnames=CSV_COLUMNS, quoting=csv.QUOTE_MINIMAL, escapechar="\\"
                )
                writer.writeheader()
                writer.writerows(self.rows.values())
            os.replace(tmp_path, self.path)
            self.dirty = 0
            return True
        except Exception as e:
            logger.error(f"Failed to save {self.path}: {e}")
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            return False


def create_session():
    session = requests.Session(impersonate="chrome120")
    session.headers.update(HEADERS)
    return session


def polite_sleep(delay):
    if delay > 0:
        time.sleep(delay + random.uniform(0, min(1.0, delay * 0.5)))


def parse_retry_after(header_value):
    if not header_value:
        return None
    header_value = header_value.strip()
    if header_value.isdigit():
        return min(int(header_value), MAX_RETRY_AFTER_SECONDS)
    try:
        dt = parsedate_to_datetime(header_value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return min(max(1, int(delta)), MAX_RETRY_AFTER_SECONDS)
    except Exception:
        return None


def fetch_page(session, url, stats, retries=MAX_RETRIES):
    blocked_once = False
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=30)
            code = resp.status_code

            if code == 200:
                return resp.text, 200
            if code == 404:
                return None, 404
            if code == 403:
                stats.http_errors += 1
                if not blocked_once:
                    blocked_once = True
                    wait = BLOCK_COOLDOWN_SECONDS + random.uniform(0, 10)
                    time.sleep(wait)
                    continue
                return None, 403
            if code == 429:
                stats.http_errors += 1
                parsed_wait = parse_retry_after(resp.headers.get("Retry-After"))
                wait = parsed_wait if parsed_wait else min(60, (2 ** attempt) + random.uniform(1, 3))
                if attempt < retries:
                    time.sleep(wait)
            elif code in (500, 502, 503, 504):
                stats.http_errors += 1
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                if attempt < retries:
                    time.sleep(wait)
            else:
                stats.http_errors += 1
                return None, code
        except Exception:
            stats.http_errors += 1
            if attempt < retries:
                time.sleep((2 ** attempt) + random.uniform(0, 1))
    return None, 0


def extract_next_data(html_text):
    if not html_text:
        return None
    soup = BeautifulSoup(html_text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    try:
        return json.loads(script.string)
    except json.JSONDecodeError:
        return None


def scrape_search_page(session, url, stats):
    empty = ([], 0, 0, 0, 0)
    html, code = fetch_page(session, url, stats)
    if code == 404:
        return ("404",) + empty
    if code == 403:
        return ("403",) + empty
    if not html:
        return ("ERROR",) + empty

    data = extract_next_data(html)
    if not isinstance(data, dict):
        return ("PARSE_ERROR",) + empty

    try:
        search_result = data["props"]["pageProps"]["searchResult"]
    except (KeyError, TypeError):
        return ("PARSE_ERROR",) + empty
    if not isinstance(search_result, dict):
        return ("PARSE_ERROR",) + empty

    raw_items = search_result.get("listings")
    meta = search_result.get("meta")
    if not isinstance(raw_items, list) or not isinstance(meta, dict):
        return ("PARSE_ERROR",) + empty

    current_page = meta.get("page")
    total_pages = meta.get("page_count")
    total_count = meta.get("total_count")
    ints = (current_page, total_pages, total_count)
    if not all(isinstance(v, int) and not isinstance(v, bool) for v in ints):
        return ("PARSE_ERROR",) + empty
    if current_page < 1 or total_pages < 1 or total_count < 0:
        return ("PARSE_ERROR",) + empty

    properties = []
    for item in raw_items:
        if isinstance(item, dict) and item.get("listing_type") == LISTING_TYPE:
            p = item.get("property")
            if isinstance(p, dict):
                properties.append(p)

    return "OK", properties, current_page, total_pages, total_count, len(raw_items)


def scrape_category(session, category, search_path, search_city, store, stats, max_pages, delay):
    logger.info(f"  -> Scraping: {category} in {search_city.upper()}")
    page = 1
    failures = 0

    while True:
        sep = "&" if "?" in search_path else "?"
        url = f"{BASE_URL}{search_path}" if page == 1 else f"{BASE_URL}{search_path}{sep}page={page}"

        status, props, curr_p, total_p, total_cnt, raw_count = scrape_search_page(session, url, stats)

        if status == "403":
            logger.error(f"Access blocked (403) on {search_city}. Stopping.")
            return "blocked"
        if status == "404":
            break
        if status != "OK":
            failures += 1
            if failures >= MAX_CONSECUTIVE_FAILURES:
                break
            polite_sleep(delay)
            continue
        failures = 0

        if page > 1 and curr_p < page:
            break

        new_c = same_c = upd_c = fail_c = 0
        for prop in props:
            stats.discovered += 1
            parsed = parse_listing(prop, category)
            if not parsed:
                stats.failed += 1
                fail_c += 1
                continue

            result, _ = store.upsert(parsed)
            if result == "new":
                stats.extracted += 1
                new_c += 1
            elif result == "updated":
                stats.updated += 1
                upd_c += 1
            else:
                stats.unchanged += 1
                same_c += 1

        logger.info(f"     Page {page:>3} ({curr_p}/{total_p}): +{new_c} new | {same_c} unchanged | {upd_c} updated | {fail_c} failed")
        store.maybe_flush()

        if raw_count == 0 or page >= total_p or (max_pages and page >= max_pages):
            break

        page += 1
        polite_sleep(delay)

    return "ok"


if __name__ == "__main__":
    landing_dir = "/Volumes/real_estate/bronze/raw_files/landing"
    state_dir = "/Volumes/real_estate/bronze/raw_files/scraper_state"
    os.makedirs(landing_dir, exist_ok=True)
    os.makedirs(state_dir, exist_ok=True)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_csv = f"{landing_dir}/pf_listings_{run_ts}.csv"
    history_csv = f"{state_dir}/propertyfinder_price_history.csv"

    store = Store(output_csv, history_csv, landing_dir)
    store.load()

    stats = Stats()
    session = create_session()
    max_pages = None
    delay = DEFAULT_DELAY

    try:
        probe, code = fetch_page(session, BASE_URL + "/en/", stats)
        if code == 200 and probe:
            for city in DEFAULT_LOCATIONS:
                blocked = False
                for category, template in URL_TEMPLATES.items():
                    outcome = scrape_category(
                        session, category, template.format(city=city), city,
                        store, stats, max_pages, delay
                    )
                    store.flush()
                    if outcome == "blocked":
                        blocked = True
                        break
                if blocked:
                    break
    finally:
        session.close()
        store.flush()

    stats.report(output_csv, store.history_recorded)