from flask import Flask, request, render_template, render_template_string, redirect, url_for
import re
import httpx
import html
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from playwright.sync_api import sync_playwright
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import time
import csv
from datetime import datetime
import os
from flask import send_file
import json
import logging
from dotenv import load_dotenv
from threading import Thread
from collections import defaultdict
import psutil
import gc
from threading import Event
import threading





CSV_DIR = "./csv_reports"
os.makedirs(CSV_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO)
load_dotenv()
added_products = []
finished = False
_job_running = Event()
MAX_JOB_RUNTIME_SECONDS = 3600
TEMP_CSV_BASENAME = "~temp_product_update_log.csv"
TEMP_CSV_PATH = os.path.join(CSV_DIR, TEMP_CSV_BASENAME)




app = Flask(__name__)

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
LOCATION_ID = int(os.getenv("LOCATION_ID"))
COSTCO_PROXY = os.getenv("COSTCO_PROXY")


HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

SITE_PARSERS = {
    "johnlewis.com": {
        "id_extractor": lambda url: re.search(r"/p(\d+)", url).group(1) if re.search(r"/p(\d+)", url) else None,
        "brand_re": r'data-testid="product:title:otherBrand"[^>]*>(.*?)</span>',
        "title_re": r'<h1[^>]*>(.*?)</h1>',
        "price_re": r'data-testid="product:basket:price"[^>]*>([£\d.,]+)</dd>',
        "stock_re": r'data-testid="product:basket:stock"[^>]*>(.*?)</span>',
        "image_re": r'<div class="Carousel_galleryItem__7ii3O[^>]*><img[^>]*src="(//media\.johnlewiscontent\.com/i/JohnLewis/.*?)"',
        "description_re": r'<div[^>]*data-testid="description:content"[^>]*>(.*?)</div>',
        "size_re": r'<dl[^>]*data-testid="basket:product:attributes:list"[^>]*>.*?<dd[^>]*class="VariantAttributes_attributeValue__5XTlL"[^>]*>(.*?)</dd>',
    }
}



def get_site_name(url):
    domain = urlparse(url).netloc
    if "johnlewis.com" in domain:
        return "johnlewis"
    elif "escentual.com" in domain:
        return "escentual"
    elif "costco.co.uk" in domain:
        return "costco"
    return "unknown"


def extract_handle_variant_sku_from_url(url):
    parsed = urlparse(url)
    path_parts = [p for p in parsed.path.split("/") if p]
    domain = parsed.netloc
    query_params = parse_qs(parsed.query)

    # 🎯 escentual.com — handle из URL, sku = variant
    if "escentual.com" in domain:
        sku = query_params.get("variant", [None])[0]
        if "products" in path_parts:
            index = path_parts.index("products")
            handle = path_parts[index + 1] if len(path_parts) > index + 1 else "unknown"
        else:
            handle = path_parts[-1] if path_parts else "unknown"
        return handle, sku, sku  # handle, variant, sku

    # 🎯 johnlewis.com — старая логика
    sku = None
    handle_parts = []
    variant = query_params.get("size", [None])[0]

    for i, part in enumerate(path_parts):
        if part.startswith("p") and part[1:].isdigit():
            sku = part[1:]
            if not variant and i > 0 and not path_parts[i - 1].startswith("p"):
                prev = path_parts[i - 1]
                if prev.replace("-", "").isalpha() and not re.search(r"\d", prev):
                    variant = None
                else:
                    variant = prev
                handle_parts = path_parts[:i - 1]
            else:
                handle_parts = path_parts[:i]
            break

    handle = "-".join(handle_parts) if handle_parts else "unknown"
    return handle, variant, sku


def load_settings(source):
    try:
        with open(os.path.join("settings", f"{source}.json"), "r", encoding="utf-8") as f:
            settings = json.load(f)
            # Обратная совместимость со старым форматом
            if "shipping_fees" not in settings and "price_range" in settings:
                match = re.match(r"(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)", settings["price_range"])
                if match:
                    min_price = float(match.group(1))
                    max_price = float(match.group(2))
                    settings["shipping_fees"] = [{
                        "price_range": [min_price, max_price],
                        "shipping_fee": float(settings.get("shipping_fee", 0))
                    }]
            return settings
    except Exception as e:
        print(f"⚠️ Ошибка загрузки настроек: {e}")
        return {"shipping_fees": [], "surcharge": False}



def parse_escentual(url):
    r = httpx.get(url, timeout=20)
    soup = BeautifulSoup(r.text, "html.parser")
    title = soup.select_one("div.product__title h1").text.strip()
    variant = soup.select_one(".variant-display--name")
    stock = soup.select_one(".variant-display--stock span")
    price = soup.select_one(".price__regular .price-item--regular")
    image_tags = soup.select(".product__media img")
    description_block = soup.select_one("div.product__description")
    description_html = str(description_block) if description_block else ""
    return {
        "title": title,
        "variant_title": variant.text.strip() if variant else "Default",
        "stock": int(re.search(r"\d+", stock.text).group()) if stock and re.search(r"\d+", stock.text) else 0,
        "price": float(re.sub(r"[^\d.]", "", price.text)) if price else 0.0,
        "image": ["https:" + i["src"] if i["src"].startswith("//") else i["src"] for i in image_tags],
        "url": url,
        "description": description_html,

    }


def parse_product(url, max_retries=3):
    print(f"\n🌐 Запрос к: {url}")
    logging.info(f"\n🌐 Запрос к: {url}")
    domain = urlparse(url).netloc.replace("www.", "")
    parser = SITE_PARSERS.get(domain)
    if not parser:
        print(f"❌ Нет парсера для: {domain}")
        logging.info(f"❌ Нет парсера для: {domain}")
        return None

    with sync_playwright() as p:
        browser = p.webkit.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        try:
            page.locator('button:has-text("Allow all")').click(timeout=3000)
        except:
            pass
        html_text = page.content()
        try:
            stock_text = page.locator('[data-testid="product:basket:stock"]').inner_text()
        except:
            stock_text = ""
        browser.close()

    title_match = re.search(parser["title_re"], html_text)
    brand_match = re.search(parser["brand_re"], html_text)
    price_match = re.search(parser["price_re"], html_text)
    image_matches = re.findall(parser["image_re"], html_text)
    variant_match = re.search(parser.get("size_re", ""), html_text, re.DOTALL)
    description_match = re.search(parser["description_re"], html_text, re.DOTALL)

    brand = html.unescape(re.sub('<.*?>', '', brand_match.group(1).strip())) if brand_match else ""

    if title_match:
        raw_title = title_match.group(1).strip()
        # Удаляем содержимое <span>...</span> из title (бренд), оставляя остальное
        raw_title = re.sub(r"<span[^>]*>.*?</span>", "", raw_title, flags=re.DOTALL)
        title = html.unescape(re.sub('<.*?>', '', raw_title)).strip()
    else:
        title = "Unknown"

    full_title = title.strip()
    base_title = full_title.split(",")[0].strip()

    price = float(re.sub(r"[^\d.]", "", price_match.group(1))) if price_match else 0.0
    variant = html.unescape(variant_match.group(1).strip()) if variant_match else None
    description_html = description_match.group(1).strip() if description_match else ""

    match = re.search(r'\d+', stock_text)
    if match:
        stock = int(match.group())
    elif "in stock" in stock_text.lower():
        stock = 10
    else:
        stock = 0

    images = ["https:" + img for img in image_matches] if image_matches else []

    handle, _, sku = extract_handle_variant_sku_from_url(url)

    return {
        "url": url,
        "handle": handle,
        "title": title,
        "base_title": base_title,
        "brand": brand,
        "full_title": full_title,
        "variant_title": variant if variant and variant.lower() != "default" else None,
        "stock": stock,
        "price": price,
        "image": images,
        "description": description_html,
        "source_variant_id": sku
    }



def calculate_final_price_create(price, settings):
    for section in settings["shipping_fees"]:
        if section["price_range"][0] <= price <= section["price_range"][1]:
            shipping_fee = float(section.get("shipping_fee", 0))
            return price + shipping_fee, shipping_fee, True, True

    # fallback для создания
    fallback_fee = 3.95 if price < 75 else 10.5
    return price + fallback_fee, fallback_fee, True, False

# ---- Costco anti-bot helpers ----
def _looks_blocked(html_text: str) -> bool:
    if not html_text:
        return True
    bad = ["access denied", "forbidden", "bot detected", "blocked", "captcha", "request unsuccessful"]
    t = html_text.lower()
    return any(x in t for x in bad)

def _parsed_is_valid(parsed: dict) -> bool:
    if not parsed:
        return False
    title = (parsed.get("title") or "").strip().lower()
    if not title or title in ("unknown", "access denied", "forbidden"):
        return False
    if (parsed.get("price") or 0.0) <= 0 and not parsed.get("image"):
        return False
    return True


def extract_variant_from_costco(url: str, html_text: str | None) -> str | None:
    """
    1) HTML: dd.product-variant-option__value / VariantAttributes_*
    2) URL slug: .../c/<slug>/p/<id>   — берём часть после '-in-' до размера/единиц
    3) ID: /p/<digits><letters>  — берём хвостовые буквы и нормализуем (DBlue -> Blue)
    """
    import re

    def _clean(v: str) -> str:
        v = re.sub(r"\s+", " ", v).strip(" -_/.,")
        # Title Case (но аккуратно с ALL CAPS)
        return " ".join(w.capitalize() if not w.isupper() else w for w in v.split())

    # 1) из HTML (если передали)
    if html_text:
        m = re.search(
            r"<dd[^>]*class=\"[^\"]*(?:product-variant-option__value|VariantAttributes_[^\"]*)[^\"]*\"[^>]*>(.*?)</dd>",
            html_text, re.IGNORECASE | re.DOTALL
        )
        if m:
            txt = re.sub(r"<.*?>", "", m.group(1), flags=re.DOTALL)
            txt = _clean(txt)
            if txt:
                return txt

    # 2) из URL slug: .../c/...-in-<color>-<digits|cm|mm|ml|pack|pair|set>
    slug_m = re.search(r"/c/([^/?#]+)/p/", url, re.IGNORECASE)
    if slug_m:
        slug = slug_m.group(1).lower()
        # ищем кусок после '-in-'
        m = re.search(
            r"(?:^|-)in-([a-z-]+?)(?:-(?:\d|cm\b|mm\b|ml\b|pack\b|pair\b|set\b))",
            slug, re.IGNORECASE
        )
        if not m:
            # fallback: всё после '-in-' до конца, но обрежем служебные хвосты
            m = re.search(r"(?:^|-)in-([a-z-]+)", slug, re.IGNORECASE)
        if m:
            raw = m.group(1)
            # сносим возможные “размерные” хвосты на всякий
            raw = re.sub(r"-(?:\d.*)$", "", raw)
            raw = raw.replace("-", " ")
            txt = _clean(raw)
            if txt:
                return txt

    # 3) из /p/<digits><letters...>
    m = re.search(r"/p/(\d+)([A-Za-z][A-Za-z0-9]*)$", url)
    if m:
        tail = m.group(2)
        # вставим пробелы между “DBlue” → “D Blue”, уберём одиночные префиксы вроде 'D '
        tail = re.sub(r"(?<!^)([A-Z])", r" \1", tail).strip()
        # часто буквенный префикс внутри SKU не нужен — оставим последнее слово как цвет
        parts = tail.split()
        if parts:
            txt = _clean(parts[-1])
            if txt:
                return txt

    return None


def parse_costco(url: str):
    """
    Costco.co.uk parser — ВСЁ через прокси:
      1) httpx ТОЛЬКО через COSTCO_PROXY (real headers + http2)
      2) если на странице есть "Online Price" → Playwright (через прокси), чтобы дождаться финальной "Your Price"
      3) если "Online Price" НЕТ → используем обычную цену сразу из httpx (PW не запускаем)
      4) если httpx не распарсили — fallback в PW

    Делает дамп проблемной HTML в ./csv_reports/costco_dump_*.html для диагностики.
    Возвращает dict или None.
    """
    import re, html, json, os
    import httpx
    from bs4 import BeautifulSoup

    proxy = (COSTCO_PROXY or "").strip() or None
    DUMP_DIR = CSV_DIR  # уже существует

    if not proxy:
        print("❌ COSTCO_PROXY не задан — принудительно работаем только через прокси, остановка.")
        return None

    # ---------- helpers ----------
    def _clip(txt: str) -> str:
        return html.unescape(re.sub(r"<.*?>", "", txt or "", flags=re.DOTALL)).strip()

    def _dump(name: str, text: str):
        try:
            path = os.path.join(DUMP_DIR, f"costco_dump_{name}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(text or "")
            print(f"📝 Costco dump saved: {path}")
        except Exception as e:
            print(f"⚠️ dump save error: {e}")

    def _looks_blocked(text: str) -> bool:
        if not text:
            return True
        low = text.lower()
        markers = [
            "access denied", "request unsuccessful", "generated by akamai", "akamai",
            "to continue, please verify", "bot detected", "forbidden", "blocked",
            "reference #", "you don't have permission", "attention required"
        ]
        return any(k in low for k in markers)

    def _to_float(s: str) -> float:
        if not s:
            return 0.0
        s = re.sub(r"[^\d.,]", "", s).replace(",", "")
        try:
            return float(s) if s else 0.0
        except:
            return 0.0

    def _extract_ld(soup: BeautifulSoup) -> dict:
        data = {}
        for tag in soup.find_all("script", type="application/ld+json"):
            raw = tag.string or ""
            try:
                j = json.loads(raw)
            except Exception:
                continue
            items = j if isinstance(j, list) else [j]
            prod = None
            for it in items:
                if isinstance(it, dict) and it.get("@type") == "Product":
                    prod = it
                if not prod and isinstance(it, dict) and isinstance(it.get("@graph"), list):
                    for node in it["@graph"]:
                        if isinstance(node, dict) and node.get("@type") == "Product":
                            prod = node
                            break
                if prod:
                    break
            if not prod:
                continue
            data.setdefault("title", prod.get("name"))
            imgs = prod.get("image")
            if imgs:
                if isinstance(imgs, str):
                    imgs = [imgs]
                norm = []
                for u in imgs:
                    if isinstance(u, str) and u:
                        if u.startswith("//"):
                            u = "https:" + u
                        norm.append(u)
                if norm:
                    data.setdefault("images", norm)
            data.setdefault("description", prod.get("description"))
            b = prod.get("brand")
            if isinstance(b, dict):
                data.setdefault("brand", b.get("name") or "")
            elif isinstance(b, str):
                data.setdefault("brand", b)
            offers = prod.get("offers")
            if isinstance(offers, dict):
                pr = offers.get("price")
                if pr and "price" not in data:
                    try:
                        data["price"] = float(str(pr).replace(",", ""))
                    except:
                        pass
                avail = (offers.get("availability") or "").lower()
                if "instock" in avail:
                    data["stock"] = 10
                elif "outofstock" in avail:
                    data["stock"] = 0
            if data:
                break
        return data

    def _extract_costco_discount_price(soup: BeautifulSoup, html_text: str) -> float | None:
        marker = soup.select_one("#__scraped_discount_price[data-price]")
        if marker:
            val = marker.get("data-price")
            if val:
                try:
                    return _to_float(val)
                except Exception:
                    pass

        node = soup.select_one(
            "div.price-after-discount span.you-pay-value, "
            "div.price-after-discount .you-pay-value span, "
            "div.price-after-discount span.notranslate"
        )
        if node:
            val = node.get_text(strip=True)
            if val:
                try:
                    return _to_float(val)
                except Exception:
                    pass

        m = re.search(
            r'class=["\']price-after-discount["\'][\s\S]*?£\s*([\d.,]+)',
            html_text, re.IGNORECASE
        )
        if m:
            try:
                return _to_float(m.group(1))
            except Exception:
                pass

        return None

    def _has_online_price_marker(html_text: str) -> bool:
        """True, если в httpx-HTML найден блок Online Price (признак скидочной страницы)."""
        if not html_text:
            return False
        if re.search(
            r'class=["\']price-original[^"\']*["\'][\s\S]*?class=["\']price-tag["\'][^>]*>\s*Online Price',
            html_text,
            re.IGNORECASE
        ):
            return True
        try:
            _soup = BeautifulSoup(html_text, "html.parser")
            tag = _soup.select_one("div.price-original .price-tag")
            return bool(tag and "online price" in tag.get_text(strip=True).lower())
        except Exception:
            return False

    # --- корректная прокси-конфигурация для Playwright ---
    def _playwright_proxy(proxy_url: str | None):
        if not proxy_url:
            return None
        try:
            import urllib.parse as up
            u = up.urlparse(proxy_url)
            server = f"{u.scheme}://{u.hostname}:{u.port}"
            cfg = {"server": server}
            if u.username:
                cfg["username"] = up.unquote(u.username)
            if u.password:
                cfg["password"] = up.unquote(u.password)
            return cfg
        except Exception:
            return {"server": proxy_url}

    def _extract_extra(
        soup: BeautifulSoup,
        html_text: str,
        *,
        refetch_html_cb=None,
        max_tries: int = 3,
        delay_seconds: float = 1.2
    ) -> dict:
        import time

        def _plain_len(html: str) -> int:
            return len(re.sub(r"<[^>]+>", "", html).strip())

        def _clean_html(html: str) -> str:
            html = re.sub(r"<(script|style|button)[\s\S]*?</\1>", "", html, flags=re.I)
            html = re.sub(r"<(div|p|span)[^>]*>\s*</\1>", "", html, flags=re.I)
            html = re.sub(r"(\s*<br\s*/?>\s*){3,}", "<br><br>", html, flags=re.I)
            html = re.sub(r"\n{3,}", "\n\n", html)
            return html.strip()

        def _pick_pdf_block(_soup: BeautifulSoup) -> str:
            pdf_ul = _soup.select_one("#product_details .pdp-pdf-bullets")
            if not pdf_ul:
                return ""
            pdf_ul = BeautifulSoup(pdf_ul.decode(), "html.parser")
            for img in pdf_ul.find_all("img"):
                img.decompose()
            return pdf_ul.decode()

        def _extract_desc_from_soup(_soup: BeautifulSoup) -> str:
            panel = _soup.select_one(
                "#product_details .pdp-tab-content-body, "
                "#product_details .product-details-content-wrapper, "
                "#product_details .product-details-wrapper, "
                "div#product_details .accordion-body, "
                "div#product_details"
            )
            pdf_block = _pick_pdf_block(_soup)
            desc_html = ""
            if panel:
                panel = BeautifulSoup(panel.decode(), "html.parser")

                ban_patterns = re.compile(
                    r"(delivery|returns|refund|shipping|specification|specifications|review|customer ratings)",
                    re.IGNORECASE
                )
                for tag in panel.find_all(True):
                    txt = tag.get_text(" ", strip=True) if tag else ""
                    if ban_patterns.search(txt or ""):
                        tag.decompose()

                for bad in panel.find_all(["script", "style", "button"]):
                    bad.decompose()
                for empty in panel.find_all(["div", "p", "span"]):
                    if not (empty.get_text(strip=True) or empty.find(["img", "ul", "ol", "table"])):
                        empty.decompose()

                body_html = panel.decode()
                if _plain_len(body_html) >= 40:
                    desc_html = (pdf_block + body_html).strip()

            if not desc_html:
                blocks = _soup.select(
                    "[data-testid*='product-details'], #product-details, .product-details, "
                    ".accordion__content, .tabs__panel, .accordion, .product-tabs"
                )
                if blocks:
                    b = BeautifulSoup(blocks[0].decode(), "html.parser")
                    for bad in b.find_all(["script", "style", "button"]):
                        bad.decompose()
                    for tag in b.find_all(True):
                        txt = tag.get_text(" ", strip=True)
                        if re.search(r"(delivery|returns|refund|shipping|specification|specifications|review)", txt, re.I):
                            tag.decompose()
                    body_html = b.decode()
                    if _plain_len(body_html) >= 40:
                        desc_html = (_pick_pdf_block(_soup) + body_html).strip()

            if not desc_html:
                for script in _soup.find_all("script", {"type": "application/ld+json"}):
                    try:
                        data = json.loads(script.string or "{}")
                        candidates = data if isinstance(data, list) else [data]
                        for obj in candidates:
                            desc = obj.get("description")
                            if desc and _plain_len(desc) >= 40:
                                desc_html = f"<div>{desc}</div>"
                                break
                        if desc_html:
                            break
                    except Exception:
                        continue

            if desc_html and _plain_len(desc_html) >= 40:
                return _clean_html(desc_html)
            return ""

        tries_left = max(1, int(max_tries))
        _soup, _html = soup, html_text
        desc_html = ""

        while tries_left > 0:
            out = {}

            m = re.search(r"<h1[^>]*>(.*?)</h1>", _html, re.IGNORECASE | re.DOTALL)
            if m:
                title_html = re.sub(r"<span[^>]*>.*?</span>", "", m.group(1), flags=re.DOTALL)
                out["title"] = _clip(title_html)

            metas = []
            for sel in [
                ('meta', {"property": "og:image"}),
                ('meta', {"name": "og:image"}),
                ('meta', {"name": "twitter:image"}),
                ('meta', {"property": "twitter:image"}),
            ]:
                for t in _soup.find_all(*sel):
                    u = t.get("content")
                    if u:
                        if u.startswith("//"):
                            u = "https:" + u
                        metas.append(u)
            for u in re.findall(r'<img[^>]+src="(https?:\/\/[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', _html, re.IGNORECASE):
                metas.append(u)
            if metas:
                out["images"] = list(dict.fromkeys(metas))

            discount_price = _extract_costco_discount_price(_soup, _html)
            if discount_price:
                out["price"] = discount_price

            if "price" not in out:
                pm = re.search(r"£\s*[\d.,]+", _html)
                if pm:
                    out["price"] = _to_float(pm.group(0))

            if "price" not in out:
                jm = re.search(r'"price"\s*:\s*"?([\d.,]+)"?', _html)
                if jm:
                    out["price"] = _to_float(jm.group(1))

            if re.search(r"Add to cart", _html, re.IGNORECASE):
                out["stock"] = 10
            elif re.search(r"Out of Stock", _html, re.IGNORECASE):
                out["stock"] = 0

            vm = re.search(
                r"<dd[^>]*class=\"[^\"]*(?:product-variant-option__value|VariantAttributes_[^\"]*)[^\"]*\"[^>]*>(.*?)</dd>",
                _html, re.IGNORECASE | re.DOTALL
            )
            if vm:
                out["variant"] = _clip(vm.group(1))

            desc_html = _extract_desc_from_soup(_soup)
            if desc_html:
                out["description"] = desc_html
                return out

            tries_left -= 1
            if tries_left <= 0 or refetch_html_cb is None:
                return out

            time.sleep(delay_seconds)
            try:
                new_html = refetch_html_cb()
                if new_html and isinstance(new_html, str) and new_html != _html:
                    _html = new_html
                    _soup = BeautifulSoup(_html, "html.parser")
            except Exception:
                return out

    # <<< fallback-экстрактор варианта >>>
    def _extract_variant_from_costco(url_: str, html_text_: str | None) -> str | None:
        def _clean(v: str) -> str:
            v = re.sub(r"\s+", " ", v).strip(" -_/.,")
            return " ".join(w.capitalize() if not w.isupper() else w for w in v.split())

        if html_text_:
            m = re.search(
                r"<dd[^>]*class=\"[^\"]*(?:product-variant-option__value|VariantAttributes_[^\"]*)[^\"]*\"[^>]*>(.*?)</dd>",
                html_text_, re.IGNORECASE | re.DOTALL
            )
            if m:
                txt = re.sub(r"<.*?>", "", m.group(1), flags=re.DOTALL)
                txt = _clean(txt)
                if txt:
                    return txt

        slug_m = re.search(r"/c/([^/?#]+)/p/", url_, re.IGNORECASE)
        if slug_m:
            slug = slug_m.group(1).lower()
            m = re.search(
                r"(?:^|-)in-([a-z-]+?)(?:-(?:\d|cm\b|mm\b|ml\b|pack\b|pair\b|set\b))",
                slug, re.IGNORECASE
            )
            if not m:
                m = re.search(r"(?:^|-)in-([a-z-]+)", slug, re.IGNORECASE)
            if m:
                raw = m.group(1)
                raw = re.sub(r"-(?:\d.*)$", "", raw)
                raw = raw.replace("-", " ")
                txt = _clean(raw)
                if txt:
                    return txt

        m = re.search(r"/p/(\d+)([A-Za-z][A-Za-z0-9]*)$", url_)
        if m:
            tail = m.group(2)
            tail = re.sub(r"(?<!^)([A-Z])", r" \1", tail).strip()
            parts = tail.split()
            if parts:
                txt = _clean(parts[-1])
                if txt:
                    return txt
        return None
    # <<< end >>>

    def _build_result(url: str, data: dict) -> dict | None:
        title = (data.get("title") or "").strip()
        if not title or title.lower() in ("unknown", "access denied", "forbidden"):
            return None
        base_title = title.split(",")[0].strip()
        handle = re.sub(r"[^a-z0-9-]+", "-", base_title.lower()).strip("-") or "unknown"
        images = list(dict.fromkeys(data.get("images") or []))
        price = float(data.get("price") or 0.0)
        stock = int(data.get("stock") or 0)
        if price <= 0 and not images:
            return None
        mid = re.search(r"/p/([A-Za-z0-9]+)$", url)
        source_id = mid.group(1) if mid else None
        return {
            "url": url,
            "handle": handle,
            "title": title,
            "base_title": base_title,
            "brand": data.get("brand") or "",
            "full_title": title,
            "variant_title": (data.get("variant") if data.get("variant") and str(data.get("variant")).lower() != "default" else None),
            "stock": stock,
            "price": price,
            "image": images,
            "description": data.get("description") or "",
            "source_variant_id": source_id
        }

    def _parse_html(url: str, html_text: str) -> dict | None:
        if _looks_blocked(html_text):
            _dump("blocked_httpx", html_text)
            return None
        soup = BeautifulSoup(html_text, "html.parser")
        data = {}
        data.update(_extract_ld(soup))
        data.update(_extract_extra(soup, html_text))

        discount_price = _extract_costco_discount_price(soup, html_text)
        if discount_price:
            data["price"] = discount_price

        if not data.get("variant"):
            data["variant"] = _extract_variant_from_costco(url, html_text)

        return _build_result(url, data)

    # ---------- Playwright (через прокси), определяем сразу, чтобы можно было вызвать в любой ветке ----------
    def _via_playwright(mobile: bool = False, use_proxy: bool = True, engine: str = "chromium") -> str | None:
        from playwright.sync_api import sync_playwright
        import random
        browser = None
        try:
            with sync_playwright() as p:
                browser_type = {"chromium": p.chromium, "webkit": p.webkit, "firefox": p.firefox}.get(engine, p.chromium)
                launch_kwargs = {
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }
                if use_proxy and proxy:
                    launch_kwargs["proxy"] = _playwright_proxy(proxy)

                print(f"[PW] launch engine={engine} mobile={mobile} proxy={launch_kwargs.get('proxy')}")
                browser = browser_type.launch(**launch_kwargs)

                ua_mobile = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                             "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")
                ctx_kwargs = {
                    "user_agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                   "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36") if not mobile else ua_mobile,
                    "locale": "en-GB",
                    "timezone_id": "Europe/London",
                    "viewport": {"width": 1366, "height": 900} if not mobile else {"width": 390, "height": 844},
                }
                if mobile:
                    ctx_kwargs.update({"device_scale_factor": 3, "is_mobile": True, "has_touch": True})

                ctx = browser.new_context(**ctx_kwargs)
                ctx.set_default_navigation_timeout(120000)
                ctx.set_default_timeout(20000)

                ctx.add_init_script("""
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
""")
                ctx.set_extra_http_headers({
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Dest": "document",
                    "Referer": "https://www.google.com/",
                })

                page = ctx.new_page()
                try:
                    page.goto(url, wait_until="networkidle")

                    for txt in ["Accept All", "Accept all", "Allow all", "Agree", "Accept", "Accept All Cookies"]:
                        try:
                            page.locator(f'button:has-text("{txt}")').click(timeout=1500)
                            break
                        except:
                            pass

                    low = (page.content() or "").lower()
                    if any(m in low for m in ["access denied", "forbidden", "attention required", "reference #", "akamai"]):
                        return page.content()

                    try:
                        page.wait_for_selector("div.price-with-discount, div.price-after-discount", timeout=6000)
                    except:
                        pass

                    try:
                        page.wait_for_selector("div.price-after-discount", timeout=6000)
                        page.wait_for_function(
                            """() => {
                                const el = document.querySelector('div.price-after-discount');
                                if (!el) return false;
                                const txt = el.textContent || '';
                                return /£\\s*\\d/.test(txt);
                            }""",
                            timeout=6000
                        )
                    except:
                        pass

                    try:
                        discount_price_text = page.evaluate("""
(() => {
  const el = document.querySelector('div.price-after-discount');
  if (!el) return null;
  const node = el.querySelector('span.you-pay-value, span.notranslate, .you-pay-value span');
  const txt = (node ? node.textContent : el.textContent) || '';
  const m = txt.match(/£\\s*[\\d.,]+/);
  return m ? m[0] : null;
})()
                        """)
                        if discount_price_text:
                            page.evaluate(f"""
(() => {{
  const price = `{discount_price_text}`.replace(/[^0-9.,]/g,'');
  let d = document.getElementById('__scraped_discount_price');
  if (!d) {{
    d = document.createElement('div');
    d.id = '__scraped_discount_price';
    d.style.display = 'none';
    document.body.appendChild(d);
  }}
  d.setAttribute('data-price', price);
}})();
                            """)
                    except:
                        pass

                    page.wait_for_timeout(500 + int(random.uniform(0, 700)))
                    return page.content()

                except Exception as e:
                    print(f"⚠️ Playwright goto/wait error: {e}")
                    try:
                        return page.content()
                    except:
                        return None
        except Exception as e:
            print(f"⚠️ Playwright launch error: {e}")
            return None
        finally:
            try:
                if browser:
                    browser.close()
            except:
                pass

    # ---------- 1) httpx ТОЛЬКО через прокси ----------
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
    }

    html_text = None
    try:
        print(f"[httpx] proxy: {proxy}")
        with httpx.Client(
            proxy=proxy,
            headers=headers,
            follow_redirects=True,
            http2=True,
            timeout=httpx.Timeout(connect=25.0, read=40.0, write=15.0, pool=20.0),
        ) as client:
            r = client.get(url)
            r.raise_for_status()
            html_text = r.text
    except Exception as e:
        print(f"⚠️ httpx Costco error: {e}")

    # --- Решение после httpx ---
    if html_text and not _looks_blocked(html_text):
        if _has_online_price_marker(html_text):
            print("↩️ На странице найден маркер 'Online Price' — переключаюсь на Playwright для финальной цены.")
            # пойдём в PW-цикл ниже
        else:
            parsed = _parse_html(url, html_text)
            if parsed:
                print("✅ Costco: httpx ok (без 'Online Price') — беру обычную цену, PW не нужен")
                return parsed
            print("⚠️ httpx без 'Online Price', но парсинг не удался — пробую Playwright.")

    # ---------- 2) Playwright — через прокси (движки x устройства) ----------
    for engine, mobile in [("chromium", False), ("webkit", False), ("chromium", True), ("webkit", True)]:
        html_pw = _via_playwright(mobile=mobile, use_proxy=True, engine=engine)
        if html_pw:
            if _looks_blocked(html_pw):
                _dump(f"blocked_pw_{engine}_{'m' if mobile else 'd'}_proxy", html_pw)
                continue
            parsed = _parse_html(url, html_pw)
            if parsed:
                print(f"✅ Costco: Playwright {engine} {'mobile' if mobile else 'desktop'} proxy ok")
                return parsed

    print("⚠️ Costco пропущен: Access Denied / Blocked")
    return None




def calculate_final_price_update(price, settings, previous_fee_applied=None, is_active_product=False):
    for section in settings["shipping_fees"]:
        if section["price_range"][0] <= price <= section["price_range"][1]:
            shipping_fee = round(float(section.get("shipping_fee", 0)), 2)
            surcharge = round(price * 0.10, 2) if is_active_product else 0.0
            total = price + shipping_fee + surcharge
            return round(total, 2), shipping_fee, surcharge, True

    if previous_fee_applied is not None:
        shipping_fee = round(previous_fee_applied, 2)
        surcharge = round(price * 0.10, 2) if is_active_product else 0.0
        total = price + shipping_fee + surcharge
        return round(total, 2), shipping_fee, surcharge, False

    return round(price, 2), 0.0, 0.0, False

def print_memory_usage(stage=""):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / 1024 / 1024
    print(f"🧠 [{stage}] Использование памяти: {mem:.2f} MB")


def create_shopify_product(parsed_list, site_name, settings):
    product_data = {
        "title": parsed_list[0].get('base_title', parsed_list[0]['title']) if site_name == "johnlewis" else
        parsed_list[0]['title'],
        "vendor": parsed_list[0].get("brand", "") or "Unknown",
        "body_html": parsed_list[0].get("description", ""),
        "tags": [site_name],
        "variants": [],
        "images": [],
        "status": "active" if settings.get("surcharge") else "draft"

    }


    has_variants = any(item.get("variant_title") for item in parsed_list)

    # 👇 добавь это
    option_name = "Colour" if site_name == "costco" else "Size"

    if has_variants:
        product_data["options"] = [{"name": option_name}]



    added_images = set()
    first_item = parsed_list[0]
    final_price, fee_applied, _, _ = calculate_final_price_create(first_item["price"], settings)




    for item in parsed_list:
        final_price = calculate_final_price_create(item["price"], settings)[0]

        images = item["image"] if isinstance(item["image"], list) else [item["image"]]
        for img in images:
            if img and img not in added_images:
                product_data["images"].append({"src": img})
                added_images.add(img)

        variant = {
            "price": str(final_price),
            "inventory_quantity": item["stock"],
            "inventory_management": "shopify",
            "inventory_policy": "deny"
        }

        if has_variants:
            variant["option1"] = item["variant_title"] or "Default Title"

        product_data["variants"].append(variant)

    try:
        print(f"\n📤 Отправка товара в Shopify: {product_data['title']}")
        logging.info(f"\n📤 Отправка товара в Shopify: {product_data['title']}")

        r = httpx.post(
            f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products.json",
            headers=HEADERS,
            json={"product": product_data},
            timeout=httpx.Timeout(120.0, connect=30.0)
        )
        r.raise_for_status()

    except httpx.ReadTimeout:
        print("❌ ReadTimeout: Shopify слишком долго отвечает. Пропуск товара.")
        logging.info("❌ ReadTimeout: Shopify слишком долго отвечает. Пропуск товара.")
        return
    except httpx.HTTPStatusError as e:
        print(f"❌ Shopify вернул ошибку {e.response.status_code}: {e.response.text}")
        logging.info(f"❌ Shopify вернул ошибку {e.response.status_code}: {e.response.text}")


        return
    except Exception as e:
        print(f"❌ Ошибка при создании товара: {e}")
        logging.info(f"❌ Ошибка при создании товара: {e}")

        return

    created_product = r.json().get("product")
    if not created_product:
        print("❌ Не удалось распарсить ответ Shopify")
        logging.info("❌ Не удалось распарсить ответ Shopify")
        return

    metafields = [
        {
            "namespace": "global",
            "key": "source_url",
            "value": parsed_list[0]["url"],
            "type": "url"
        },
        {
            "namespace": "global",
            "key": "shipping_fee_applied",
            "value": str(fee_applied),
            "type": "number_decimal"
        }
    ]

    for metafield in metafields:
        print(f"📦 Добавление метаполя: {metafield['key']} = {metafield['value']}")
        logging.info(f"📦 Добавление метаполя: {metafield['key']} = {metafield['value']}")


        try:
            meta_resp = httpx.post(
                f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{created_product['id']}/metafields.json",
                headers=HEADERS,
                json={"metafield": metafield},
                timeout=20
            )
            if meta_resp.status_code == 201:
                print(f"✅ Метаполе {metafield['key']} успешно добавлено")
                logging.info(f"✅ Метаполе {metafield['key']} успешно добавлено")

            else:
                print(f"⚠️ Не удалось добавить метаполе {metafield['key']}: {meta_resp.text}")
                logging.info(f"⚠️ Не удалось добавить метаполе {metafield['key']}: {meta_resp.text}")

        except Exception as e:
            print(f"❌ Ошибка при добавлении метаполя {metafield['key']}: {e}")
            logging.info(f"❌ Ошибка при добавлении метаполя {metafield['key']}: {e}")


def process_links_in_background(links, escentual_settings, johnlewis_settings, costco_settings):  # 👈 ДОБАВИЛИ costco_settings
    global added_products, finished
    added_products = []
    finished = False

    try:
        for link in links:
            site = get_site_name(link)

            if site == "escentual":
                parsed = parse_escentual(link)
                if parsed:
                    print(f"\n🔍 Обработка escentual: {parsed['title']}")
                    create_shopify_product([parsed], "escentual", escentual_settings)
                    added_products.append({
                        "title": parsed.get("title", "Untitled"),
                        "link": link,
                        "price": parsed.get("price", "-"),
                        "variant": parsed.get("variant_title") or "-",
                        "quantity": parsed.get("stock", "-")
                    })

            elif site == "johnlewis":
                parsed = parse_product(link)  # ← как просил, ничего не меняем
                if parsed:
                    product_name = parsed.get("base_title") or parsed.get("title") or "Без названия"
                    print(f"\n🔍 Обработка johnlewis: {product_name}")
                    create_shopify_product([parsed], "johnlewis", johnlewis_settings)
                    added_products.append({
                        "title": product_name,
                        "link": link,
                        "price": parsed.get("price", "-"),
                        "variant": parsed.get("variant_title") or "-",
                        "quantity": parsed.get("stock", "-")
                    })



            elif site == "costco":

                try:

                    parsed = parse_costco(link)

                    if parsed and _parsed_is_valid(parsed):

                        product_name = parsed.get("base_title") or parsed.get("title") or "Без названия"

                        print(f"\n🔍 Обработка costco: {product_name}")

                        create_shopify_product([parsed], "costco", costco_settings)

                        added_products.append({

                            "title": product_name,

                            "link": link,

                            "price": parsed.get("price", "-"),

                            "variant": parsed.get("variant_title") or "-",

                            "quantity": parsed.get("stock", "-")

                        })

                    else:

                        print("⚠️ Costco пропущен: Access Denied / Blocked")

                        added_products.append({

                            "title": "[SKIPPED] Costco - Access Denied / Blocked",

                            "link": link, "price": "-", "variant": "-", "quantity": 0

                        })

                except Exception as e:

                    print(f"❌ Costco парсер кинул исключение (перехвачено): {e}")

                    added_products.append({

                        "title": "[ERROR] Costco parsing failed",

                        "link": link, "price": "-", "variant": "-", "quantity": 0

                    })



    finally:
        print("✅ Все товары добавлены, выставляем finished = True")
        logging.info("✅ Все товары добавлены, выставляем finished = True")
        finished = True



def update_all_products_from_escentual():
    print("🔁 Начало обновления товаров из escentual.com...")
    logging.info("🔁 Начало обновления товаров из escentual.com...")

    base_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products.json?limit=250"
    all_products = []
    next_url = base_url

    while next_url:
        response = httpx.get(next_url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Ошибка при получении товаров: {response.status_code}")
            logging.info(f"❌ Ошибка при получении товаров: {response.status_code}")
            break
        data = response.json().get("products", [])
        all_products.extend(data)

        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            next_part = [l for l in link_header.split(',') if 'rel="next"' in l]
            next_url = next_part[0].split(";")[0].strip()[1:-1] if next_part else None
        else:
            next_url = None

    print(f"✅ Получено товаров: {len(all_products)}")
    logging.info(f"✅ Получено товаров: {len(all_products)}")

    updated_count = 0
    for product in all_products:
        if "escentual" not in product.get("tags", ""):
            continue

        handle = product["handle"]
        for variant in product.get("variants", []):
            variant_id = variant["id"]
            inventory_item_id = variant["inventory_item_id"]

            # Получаем метаполе source_url и shipping_fee_applied
            metafields_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{product['id']}/metafields.json"
            response = httpx.get(metafields_url, headers=HEADERS)
            if response.status_code != 200:
                print(f"⚠️ Не удалось получить метафилды для варианта {variant_id}")
                logging.info(f"⚠️ Не удалось получить метафилды для варианта {variant_id}")
                continue

            metafields = response.json().get("metafields", [])
            source_url = None
            previous_fee_applied = 0.0

            for m in metafields:
                if m["namespace"] == "global" and m["key"] == "source_url":
                    source_url = m["value"]
                elif m["namespace"] == "global" and m["key"] == "shipping_fee_applied":
                    try:
                        previous_fee_applied = float(m["value"])
                    except:
                        previous_fee_applied = 0.0

            print(f"🔍 Проверка варианта ID {variant_id}...")
            logging.info(f"🔍 Проверка варианта ID {variant_id}...")

            if not source_url:
                print(f"ℹ️ Пропущен: не найден source_url для варианта {variant_id}")
                logging.info(f"ℹ️ Пропущен: не найден source_url для варианта {variant_id}")
                continue

            try:
                response = httpx.get(source_url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print(f"❌ Ошибка запроса к {source_url}: {e}")
                logging.info(f"❌ Ошибка запроса к {source_url}: {e}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            price_tag = soup.select_one(".price__regular .price-item--regular")
            stock_tag = soup.select_one(".variant-display--stock span")

            price = float(re.sub(r"[^\d.]", "", price_tag.text)) if price_tag else 0.0
            stock = int(m.group()) if stock_tag and (m := re.search(r"\d+", stock_tag.text)) else 0

            # Загружаем настройки
            settings = load_settings("escentual")

            # Вычисляем новую цену и fee
            is_active = product.get("status") == "active"

            # Вычисляем новую цену
            final_price, fee_applied, surcharge_applied, used_section_fee = calculate_final_price_update(
                price, settings, previous_fee_applied, is_active_product=is_active
            )

            print("\n📦 Обновление варианта Escentual:")
            logging.info("\n📦 Обновление варианта Escentual:")
            print(f"🔖 Продукт: {product['title']}")
            print(f"🧾 Вариант: {variant.get('title')}")
            print(f"🌍 Source URL: {source_url}")
            print(f"💲 Исходная цена с сайта: {price}")
            print(f"📦 Остаток: {stock}")
            print(f"🔧 Применён shipping fee: {fee_applied}")
            print(f"➕ Надбавка (surcharge): {'да' if surcharge_applied else 'нет'}")
            print(f"✅ Новая цена: {final_price}")

            # Обновляем цену в Shopify
            httpx.put(
                f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}.json",
                headers=HEADERS,
                json={"variant": {"price": final_price}}
            )

            # Обновляем метафилд shipping_fee_applied, если fee из секции
            if fee_applied != previous_fee_applied:
                print(f"✏️ Обновление метафилда shipping_fee_applied на {fee_applied}")
                logging.info(f"✏️ Обновление метафилда shipping_fee_applied на {fee_applied}")
                metafield_payload = {
                    "metafield": {
                        "namespace": "global",
                        "key": "shipping_fee_applied",
                        "value": str(fee_applied),
                        "type": "number_decimal"
                    }
                }
                httpx.post(
                    f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{product['id']}/metafields.json",
                    headers=HEADERS,
                    json=metafield_payload
                )

            # Обновляем количество
            inventory_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/inventory_levels/set.json"
            inventory_payload = {
                "location_id": LOCATION_ID,
                "inventory_item_id": inventory_item_id,
                "available": stock
            }
            httpx.post(inventory_url, headers=HEADERS, json=inventory_payload)

            # CSV лог
            log_product_to_csv(
                sku=variant.get("sku"),
                title=product["title"],
                variant=variant.get("title"),
                price=price,
                quantity=stock,
                tag="escentual",
                shipping_fee=fee_applied
            )

            print(f"✅ Обновлено: {handle} | Цена: {price} | Остаток: {stock}")
            logging.info(f"✅ Обновлено: {handle} | Цена: {price} | Остаток: {stock}")

            updated_count += 1

    print(f"✅ Обновление завершено. Всего обновлено вариантов: {updated_count}")
    logging.info(f"✅ Обновление завершено. Всего обновлено вариантов: {updated_count}")


def update_all_products_from_johnlewis():
    from playwright.sync_api import sync_playwright
    import gc

    print("🔁 Обновление товаров с тегом johnlewis...")

    base_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products.json?limit=250"
    all_products = []
    next_url = base_url

    while next_url:
        response = httpx.get(next_url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Ошибка при получении товаров: {response.status_code}")
            break
        data = response.json().get("products", [])
        all_products.extend(data)

        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            next_part = [l for l in link_header.split(',') if 'rel="next"' in l]
            next_url = next_part[0].split(";")[0].strip()[1:-1] if next_part else None
        else:
            next_url = None

    updated_count = 0

    for product in all_products:
        if "johnlewis" not in product.get("tags", ""):
            continue

        for variant in product.get("variants", []):
            variant_id = variant["id"]
            inventory_item_id = variant["inventory_item_id"]

            metafields_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{product['id']}/metafields.json"
            response = httpx.get(metafields_url, headers=HEADERS)
            if response.status_code != 200:
                continue

            metafields = response.json().get("metafields", [])
            source_url = None
            previous_fee_applied = 0.0

            for m in metafields:
                if m["namespace"] == "global" and m["key"] == "source_url":
                    source_url = m["value"]
                elif m["namespace"] == "global" and m["key"] == "shipping_fee_applied":
                    try:
                        previous_fee_applied = float(m["value"])
                    except:
                        previous_fee_applied = 0.0

            if not source_url:
                continue

            print(f"🌐 Ссылка для парсинга: {source_url}")

            try:
                with sync_playwright() as p:
                    browser = p.webkit.launch(headless=True)
                    page = browser.new_page()
                    page.route("**/*", lambda route, request: route.abort()
                               if request.resource_type in ["image", "stylesheet", "media", "font"]
                               else route.continue_())

                    page.goto(source_url, timeout=60000)
                    try:
                        page.locator('button:has-text("Allow all")').click(timeout=3000)
                    except:
                        pass

                    try:
                        page.wait_for_selector('[data-testid="product:basket:stock"]', timeout=10000)
                        stock_text = page.locator('[data-testid="product:basket:stock"]').inner_text()
                        match = re.search(r'\d+', stock_text)
                        stock = int(match.group()) if match else 10 if "in stock" in stock_text.lower() else 0
                    except:
                        stock = 0

                    try:
                        price_text = page.locator('[data-testid="product:basket:price"]').inner_text()
                        price = float(re.sub(r"[^\d.]", "", price_text)) if price_text else 0.0
                    except:
                        price = 0.0

                    page.close()
                    browser.close()

                settings = load_settings("johnlewis")
                is_active = product.get("status") == "active"

                final_price, fee_applied, surcharge_applied, used_section_fee = calculate_final_price_update(
                    price, settings, previous_fee_applied, is_active_product=is_active
                )

                httpx.put(
                    f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}.json",
                    headers=HEADERS,
                    json={"variant": {"price": final_price}}
                )

                if used_section_fee:
                    print(f"✏️ Обновление метафилда shipping_fee_applied на {fee_applied}")
                    logging.info(f"✏️ Обновление метафилда shipping_fee_applied на {fee_applied}")
                    metafield_payload = {
                        "metafield": {
                            "namespace": "global",
                            "key": "shipping_fee_applied",
                            "value": str(fee_applied),
                            "type": "number_decimal"
                        }
                    }
                    httpx.post(
                        f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{product['id']}/metafields.json",
                        headers=HEADERS,
                        json=metafield_payload
                    )

                httpx.post(
                    f"{SHOPIFY_STORE_URL}/admin/api/2024-01/inventory_levels/set.json",
                    headers=HEADERS,
                    json={
                        "location_id": LOCATION_ID,
                        "inventory_item_id": inventory_item_id,
                        "available": stock
                    }
                )

                log_product_to_csv(
                    sku=variant.get("sku"),
                    title=product["title"],
                    variant=variant.get("title"),
                    price=price,
                    quantity=stock,
                    tag="johnlewis",
                    shipping_fee=fee_applied
                )

                updated_count += 1
                gc.collect()

            except Exception as e:
                print(f"❌ Ошибка при обновлении товара: {e}")
                continue

    print(f"\n✅ Обновление завершено. Всего обновлено вариантов: {updated_count}")


def update_all_products_from_costco():
    print("🔁 Начало обновления товаров из costco.co.uk...")
    logging.info("🔁 Начало обновления товаров из costco.co.uk...")

    base_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products.json?limit=250"
    all_products = []
    next_url = base_url

    while next_url:
        response = httpx.get(next_url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Ошибка при получении товаров: {response.status_code}")
            logging.info(f"❌ Ошибка при получении товаров: {response.status_code}")
            break
        data = response.json().get("products", [])
        all_products.extend(data)

        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            next_part = [l for l in link_header.split(',') if 'rel="next"' in l]
            next_url = next_part[0].split(";")[0].strip()[1:-1] if next_part else None
        else:
            next_url = None

    print(f"✅ Получено товаров: {len(all_products)}")
    logging.info(f"✅ Получено товаров: {len(all_products)}")

    updated_count = 0
    settings = load_settings("costco")

    for product in all_products:
        if "costco" not in product.get("tags", ""):
            continue

        handle = product["handle"]
        is_active = product.get("status") == "active"

        # product-level metafields
        metafields_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{product['id']}/metafields.json"
        resp_m = httpx.get(metafields_url, headers=HEADERS)
        if resp_m.status_code != 200:
            print(f"⚠️ Не удалось получить метаполя: {handle}")
            continue

        metafields = resp_m.json().get("metafields", [])
        source_url, previous_fee_applied = None, 0.0
        for m in metafields:
            if m["namespace"] == "global" and m["key"] == "source_url":
                source_url = m["value"]
            elif m["namespace"] == "global" and m["key"] == "shipping_fee_applied":
                try:
                    previous_fee_applied = float(m["value"])
                except:
                    previous_fee_applied = 0.0

        if not source_url:
            print(f"ℹ️ Пропуск: нет source_url для {handle}")
            continue

        # парсинг Costco (без Playwright)
        parsed = parse_costco(source_url)
        if not parsed:
            print(f"❌ Не удалось распарсить Costco: {source_url}")
            continue

        price = parsed.get("price", 0.0)
        stock = parsed.get("stock", 0)

        final_price, fee_applied, surcharge_applied, used_section_fee = calculate_final_price_update(
            price, settings, previous_fee_applied, is_active_product=is_active
        )

        for variant in product.get("variants", []):
            variant_id = variant["id"]
            inventory_item_id = variant["inventory_item_id"]

            # обновляем цену
            httpx.put(
                f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}.json",
                headers=HEADERS,
                json={"variant": {"price": final_price}}
            )

            # обновляем shipping_fee_applied, если изменился
            if used_section_fee and fee_applied != previous_fee_applied:
                print(f"✏️ Обновление метафилда shipping_fee_applied на {fee_applied}")
                logging.info(f"✏️ Обновление метафилда shipping_fee_applied на {fee_applied}")
                metafield_payload = {
                    "metafield": {
                        "namespace": "global",
                        "key": "shipping_fee_applied",
                        "value": str(fee_applied),
                        "type": "number_decimal"
                    }
                }
                httpx.post(
                    f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products/{product['id']}/metafields.json",
                    headers=HEADERS,
                    json=metafield_payload
                )

            # обновляем остаток
            httpx.post(
                f"{SHOPIFY_STORE_URL}/admin/api/2024-01/inventory_levels/set.json",
                headers=HEADERS,
                json={
                    "location_id": LOCATION_ID,
                    "inventory_item_id": inventory_item_id,
                    "available": stock
                }
            )

            # CSV лог
            log_product_to_csv(
                sku=variant.get("sku"),
                title=product["title"],
                variant=variant.get("title"),
                price=price,
                quantity=stock,
                tag="costco",
                shipping_fee=fee_applied
            )

            updated_count += 1

    print(f"✅ Обновление Costco завершено. Всего обновлено вариантов: {updated_count}")
    logging.info(f"✅ Обновление Costco завершено. Всего обновлено вариантов: {updated_count}")





def run_all_updates():
    """
    Последовательное обновление всех источников (Escentual, JohnLewis)
    с защитой от залипания флага и корректной работой с временным CSV.
    """
    if _job_running.is_set():
        if hasattr(_job_running, "start_time"):
            elapsed = time.time() - _job_running.start_time
            if elapsed > MAX_JOB_RUNTIME_SECONDS:
                logging.warning("⚠️ Флаг выполнения залип (>1 час). Сбрасываем и запускаем задачу.")
                _job_running.clear()
            else:
                logging.info("⏭️ Задача ещё выполняется — пропускаем запуск.")
                return
        else:
            logging.info("⏭️ Задача ещё выполняется — пропускаем запуск.")
            return

    _job_running.set()
    _job_running.start_time = time.time()

    try:
        logging.info("🚀 Запуск последовательного обновления всех источников...")

        # 1) Удаляем ЛЮБОЙ старый временный файл(ы), чтобы не сливать логи предыдущих падений
        try:
            for f in os.listdir(CSV_DIR):
                if f.startswith("~temp_product_update_log") and f.endswith(".csv"):
                    try:
                        os.remove(os.path.join(CSV_DIR, f))
                        logging.info(f"🧹 Удалён старый временный CSV: {f}")
                    except Exception as e:
                        logging.warning(f"⚠️ Не удалось удалить старый временный CSV {f}: {e}")
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при очистке временных CSV: {e}")

        # 2) Стартуем с НОВЫМ чистым временным файлом
        log_product_to_csv.filename = TEMP_CSV_PATH

        # --- Основные шаги обновления ---
        try:
            update_all_products_from_escentual()
            update_all_products_from_johnlewis()
            update_all_products_from_costco()
        except Exception:
            logging.exception("❌ Ошибка во время run_all_updates")
        # --------------------------------

        # 3) Если что-то записали — переименовываем во финальный атомарно
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        final_filename = os.path.join(CSV_DIR, f"product_update_log_{timestamp}.csv")

        if os.path.exists(TEMP_CSV_PATH):
            # чистим старые финальные перед записью нового (как у тебя было)
            for f in os.listdir(CSV_DIR):
                if f.startswith("product_update_log_") and f.endswith(".csv") and not f.startswith("~"):
                    try:
                        os.remove(os.path.join(CSV_DIR, f))
                    except Exception:
                        pass

            # атомарная замена (работает, даже если файл уже существует)
            try:
                os.replace(TEMP_CSV_PATH, final_filename)
            except Exception:
                # на всякий случай fallback
                os.rename(TEMP_CSV_PATH, final_filename)

            log_product_to_csv.filename = final_filename
            logging.info(f"✅ CSV-файл обновлён и готов к скачиванию: {final_filename}")
        else:
            logging.info("ℹ️ Обновлений не было — лог не создан, CSV не требуется.")
            log_product_to_csv.filename = None

    finally:
        _job_running.clear()
        if hasattr(_job_running, "start_time"):
            del _job_running.start_time
        logging.info("🔚 Задача run_all_updates завершена, флаг сброшен.")



def log_product_to_csv(sku: str, title: str, variant: str, price: float, quantity: int, tag: str, shipping_fee: float = None):
    # Лок на случай параллельных записей
    if not hasattr(log_product_to_csv, "_lock"):
        log_product_to_csv._lock = threading.Lock()

    with log_product_to_csv._lock:
        # Если filename не задан — пишем в временный по умолчанию
        if not hasattr(log_product_to_csv, "filename") or log_product_to_csv.filename is None:
            log_product_to_csv.filename = TEMP_CSV_PATH

        filename = log_product_to_csv.filename
        file_exists = os.path.isfile(filename)

        variant_cleaned = "-" if not variant or variant.strip().lower() == "default title" else variant

        row = [
            sku or "-",
            title or "-",
            variant_cleaned,
            price,
            shipping_fee if shipping_fee is not None else "-",
            quantity,
            tag
        ]

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "SKU", "Title/Product Name", "Variation",
                    "Supplier's Cost Price", "Supplier's Shipping Fee",
                    "Quantity", "Tags"
                ])
            writer.writerow(row)
            logging.info(f"📄 CSV строка добавлена: {row}")




@app.route("/download_csv")
def download_csv():
    # Ищем последний завершённый CSV (не временный)
    all_files = [
        f for f in os.listdir(CSV_DIR)
        if f.startswith("product_update_log_") and f.endswith(".csv") and not f.startswith("~")
    ]
    if not all_files:
        return """
        <html>
        <head><title>CSV Not Ready</title></head>
        <body style="font-family: sans-serif; background: #f4f6f8; padding: 40px;">
            <h2>CSV file is not yet generated</h2>
            <p>Please wait until the next product update is complete.</p>
            <a href="/" style="color: #2a72d4; text-decoration: underline;">Return to home</a>
        </body>
        </html>
        """

    # Сортируем по времени (по имени), берём самый свежий
    all_files.sort(reverse=True)
    latest_file = os.path.join(CSV_DIR, all_files[0])

    print(f"⬇️ Скачивание файла: {latest_file}")
    logging.info(f"⬇️ Скачивание файла: {latest_file}")
    return send_file(latest_file, as_attachment=True)


@app.route("/save_settings/<source>", methods=["POST"])
def save_settings(source):
    if source not in ["escentual", "johnlewis", "costco"]:
        return {"success": False, "message": "Unknown source"}, 400

    data = request.get_json()
    filepath = os.path.join("settings", f"{source}.json")

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return {"success": True}
    except Exception as e:
        print(f"❌ Ошибка сохранения {source}.json: {e}")
        logging.info(f"❌ Ошибка сохранения {source}.json: {e}")
        return {"success": False}, 500


@app.route("/status")
def status():
    global added_products, finished
    print(f"📡 Запрос к /status | finished = {finished}")
    return jsonify({
        "products": added_products,
        "finished": finished
    })

from flask import Flask, request, render_template, jsonify
from threading import Thread

@app.route("/", methods=["GET", "POST"])
def index():
    global added_products, finished
    escentual_settings = load_settings("escentual")
    johnlewis_settings = load_settings("johnlewis")
    costco_settings = load_settings("costco")   # 👈 ДОБАВЬ
    added_products.clear()
    finished = False


    status = ""

    def is_settings_valid_any(settings):
        # новый формат — наличие списка shipping_fees
        return bool(settings.get("shipping_fees"))

    if request.method == "POST":
        raw = request.form.get("links", "")
        links = [l.strip() for l in raw.splitlines() if l.startswith("https://")]

        has_costco = any("costco.co.uk" in l for l in links)

        if not is_settings_valid_any(escentual_settings) or not is_settings_valid_any(johnlewis_settings) or (has_costco and not is_settings_valid_any(costco_settings)):
            return jsonify({"success": False, "error": "❌ Specify settings for all used sources (escentual/johnlewis/costco)."}), 400

        thread = Thread(target=process_links_in_background, args=(links, escentual_settings, johnlewis_settings, costco_settings))  # 👈 ДОБАВИЛИ costco_settings
        thread.start()

        return jsonify({"success": True, "message": "✅ Добавление запущено."}), 200

    return render_template("index.html",
                           status=status,
                           escentual_settings=escentual_settings,
                           johnlewis_settings=johnlewis_settings,
                           costco_settings=costco_settings,   # 👈 по желанию на UI
                           added_products=added_products)



executors = {'default': ThreadPoolExecutor(10)}

scheduler = BackgroundScheduler(
    executors=executors,
    job_defaults={
        'coalesce': True,
        'max_instances': 5,   # было 1 — даём шанс стартовать "пустышкам"
        'misfire_grace_time': 600
    }
)

scheduler.add_job(
    func=run_all_updates,
    trigger='interval',
    minutes=20,
    id='run_all_updates',
    replace_existing=True,
    max_instances=5          # на уровне job тоже можно явно указать
)
scheduler.start()


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))