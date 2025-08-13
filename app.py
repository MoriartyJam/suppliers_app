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





CSV_DIR = "./csv_reports"
os.makedirs(CSV_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO)
load_dotenv()
added_products = []
finished = False
_job_running = Event()
MAX_JOB_RUNTIME_SECONDS = 3600




app = Flask(__name__)

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
LOCATION_ID = int(os.getenv("LOCATION_ID"))


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
    if has_variants:
        product_data["options"] = [{"name": "Size"}]

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


def process_links_in_background(links, escentual_settings, johnlewis_settings):
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
                        "variant": parsed.get("option1") or "-",
                        "quantity": parsed.get("stock", "-")
                    })

            elif site == "johnlewis":
                parsed = parse_product(link)
                if parsed:
                    product_name = parsed.get("base_title") or parsed.get("title") or "Без названия"
                    print(f"\n🔍 Обработка johnlewis: {product_name}")
                    create_shopify_product([parsed], "johnlewis", johnlewis_settings)
                    added_products.append({
                        "title": product_name,
                        "link": link,
                        "price": parsed.get("price", "-"),
                        "variant": parsed.get("option1") or "-",
                        "quantity": parsed.get("stock", "-")
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



def run_all_updates():
    """
    Последовательное обновление всех источников (Escentual, JohnLewis)
    с защитой от залипания флага и автоматическим созданием CSV.
    """
    # Проверяем, не выполняется ли уже задача
    if _job_running.is_set():
        # Дополнительно проверяем, не залип ли флаг
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

    # Устанавливаем флаг и сохраняем время старта
    _job_running.set()
    _job_running.start_time = time.time()

    try:
        print("🚀 Запуск последовательного обновления всех источников...")
        logging.info("🚀 Запуск последовательного обновления всех источников...")

        # Создаём временный CSV-файл
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        temp_filename = os.path.join(CSV_DIR, f"~temp_product_update_log.csv")
        log_product_to_csv.filename = temp_filename

        # --- Основные шаги обновления ---
        try:
            # Твои функции обновления — подставь свои реализации
            update_all_products_from_escentual()
            update_all_products_from_johnlewis()
        except Exception:
            logging.exception("❌ Ошибка во время run_all_updates")
        # --------------------------------

        # Переименовываем временный файл после успешного обновления
        final_filename = os.path.join(CSV_DIR, f"product_update_log_{timestamp}.csv")

        if os.path.exists(temp_filename):
            # Удаляем старые CSV-файлы
            for f in os.listdir(CSV_DIR):
                if f.startswith("product_update_log_") and not f.startswith("~") and f.endswith(".csv"):
                    try:
                        os.remove(os.path.join(CSV_DIR, f))
                    except Exception:
                        pass

            os.rename(temp_filename, final_filename)
            log_product_to_csv.filename = final_filename
            print(f"✅ CSV-файл обновлён и готов к скачиванию: {final_filename}")
            logging.info(f"✅ CSV-файл обновлён и готов к скачиванию: {final_filename}")
        else:
            print("ℹ️ Обновлений не было — лог не создан, CSV не требуется.")
            logging.info("ℹ️ Обновлений не было — лог не создан, CSV не требуется.")
            log_product_to_csv.filename = None

    finally:
        # Обязательно сбрасываем флаг в любом случае
        _job_running.clear()
        if hasattr(_job_running, "start_time"):
            del _job_running.start_time
        print("🔚 Задача run_all_updates завершена, флаг сброшен.")
        logging.info("🔚 Задача run_all_updates завершена, флаг сброшен.")


def log_product_to_csv(sku: str, title: str, variant: str, price: float, quantity: int, tag: str, shipping_fee: float = None):
    if not hasattr(log_product_to_csv, "filename") or log_product_to_csv.filename is None:
        # Удаляем старые CSV
        for old_file in os.listdir(CSV_DIR):
            if old_file.startswith("product_update_log_") and old_file.endswith(".csv"):
                os.remove(os.path.join(CSV_DIR, old_file))
        # Создаём новый
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_product_to_csv.filename = os.path.join(CSV_DIR, f"product_update_log_{timestamp}.csv")

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

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(
                ["SKU", "Title/Product Name", "Variation", "Supplier's Cost Price", "Supplier's Shipping Fee",
                 "Quantity", "Tags"])
        writer.writerow(row)
        print(f"📄 CSV строка добавлена: {row}")
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
    if source not in ["escentual", "johnlewis"]:
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
    added_products.clear()
    finished = False


    status = ""

    def is_settings_valid(settings):
        return bool(settings.get("price_range")) and settings.get("shipping_fee") not in [None, ""]

    if request.method == "POST":
        if not is_settings_valid(escentual_settings) or not is_settings_valid(johnlewis_settings):
            return jsonify({"success": False, "error": "❌ Specify settings in both sections."}), 400

        raw = request.form.get("links", "")
        links = [l.strip() for l in raw.splitlines() if l.startswith("https://")]

        thread = Thread(target=process_links_in_background, args=(links, escentual_settings, johnlewis_settings))
        thread.start()

        return jsonify({"success": True, "message": "✅ Добавление запущено."}), 200

    return render_template("index.html",
                           status=status,
                           escentual_settings=escentual_settings,
                           johnlewis_settings=johnlewis_settings,
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
