from flask import Flask, request, render_template_string
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

CSV_DIR = "./csv_reports"
os.makedirs(CSV_DIR, exist_ok=True)

app = Flask(__name__)

SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
LOCATION_ID = os.getenv('LOCATION_ID')

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
        "title": title.split(",")[0],
        "variant_title": variant.text.strip() if variant else "Default",
        "stock": int(re.search(r"\d+", stock.text).group()) if stock else 0,
        "price": float(re.sub(r"[^\d.]", "", price.text)) if price else 0.0,
        "image": ["https:" + i["src"] if i["src"].startswith("//") else i["src"] for i in image_tags],
        "url": url,
        "description": description_html,

    }


def parse_product(url, max_retries=3):
    print(f"\n🌐 Запрос к: {url}")
    domain = urlparse(url).netloc.replace("www.", "")
    parser = SITE_PARSERS.get(domain)
    if not parser:
        print(f"❌ Нет парсера для: {domain}")
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


def create_shopify_product(parsed_list, site_name):
    product_data = {
        "title": parsed_list[0].get('base_title', parsed_list[0]['title']) if site_name == "johnlewis" else
        parsed_list[0]['title'],
        "vendor": parsed_list[0].get("brand", "") or "Unknown",
        "body_html": parsed_list[0].get("description", ""),
        "tags": [site_name],
        "variants": [],
        "images": [],
    }

    has_variants = any(item.get("variant_title") for item in parsed_list)
    if has_variants:
        product_data["options"] = [{"name": "Size"}]

    added_images = set()
    for item in parsed_list:
        images = item["image"] if isinstance(item["image"], list) else [item["image"]]
        for img in images:
            if img and img not in added_images:
                product_data["images"].append({"src": img})
                added_images.add(img)

        variant = {
            "price": str(item["price"]),
            "inventory_quantity": item["stock"],
            "inventory_management": "shopify",
            "inventory_policy": "deny"
        }

        if has_variants:
            variant["option1"] = item["variant_title"] or "Default Title"

        product_data["variants"].append(variant)

    try:
        print(f"\n📤 Отправка товара в Shopify: {product_data['title']}")
        r = httpx.post(
            f"{SHOPIFY_STORE_URL}/admin/api/2024-01/products.json",
            headers=HEADERS,
            json={"product": product_data},
            timeout=httpx.Timeout(120.0, connect=30.0)
        )
        r.raise_for_status()

    except httpx.ReadTimeout:
        print("❌ ReadTimeout: Shopify слишком долго отвечает. Пропуск товара.")
        return
    except httpx.HTTPStatusError as e:
        print(f"❌ Shopify вернул ошибку {e.response.status_code}: {e.response.text}")
        return
    except Exception as e:
        print(f"❌ Ошибка при создании товара: {e}")
        return

    created_product = r.json().get("product")
    if not created_product:
        print("❌ Не удалось распарсить ответ Shopify")
        return

    for idx, variant in enumerate(created_product["variants"]):
        meta = {
            "metafield": {
                "namespace": "global",
                "key": "source_url",
                "value": parsed_list[idx]["url"],
                "type": "url"
            }
        }
        try:
            meta_resp = httpx.post(
                f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant['id']}/metafields.json",
                headers=HEADERS,
                json=meta,
                timeout=20
            )
            if meta_resp.status_code == 201:
                print(f"✅ Метаполе добавлено для варианта {variant['id']}")
            else:
                print(f"⚠️ Не удалось добавить метаполе для варианта {variant['id']}: {meta_resp.text}")
        except Exception as e:
            print(f"❌ Ошибка при добавлении метаполя: {e}")


def update_all_products_from_escentual():
    print("🔁 Начало обновления товаров из escentual.com...")
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

    print(f"✅ Получено товаров: {len(all_products)}")

    updated_count = 0
    for product in all_products:
        if "escentual" not in product.get("tags", ""):
            continue

        handle = product["handle"]
        for variant in product.get("variants", []):
            variant_id = variant["id"]
            inventory_item_id = variant["inventory_item_id"]

            # Получаем метаполе source_url (которое ты сохраняешь при создании)
            metafields_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}/metafields.json"
            response = httpx.get(metafields_url, headers=HEADERS)
            if response.status_code != 200:
                print(f"⚠️ Не удалось получить метафилды для варианта {variant_id}")
                continue

            metafields = response.json().get("metafields", [])
            source_url = next(
                (m["value"] for m in metafields if m["namespace"] == "global" and m["key"] == "source_url"), None)
            print(f"🔍 Проверка варианта ID {variant_id}...")

            if not source_url:
                print(f"ℹ️ Пропущен: не найден source_url для варианта {variant_id}")
                continue

            try:
                response = httpx.get(source_url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print(f"❌ Ошибка запроса к {source_url}: {e}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            price_tag = soup.select_one(".price__regular .price-item--regular")
            stock_tag = soup.select_one(".variant-display--stock span")

            price = float(re.sub(r"[^\d.]", "", price_tag.text)) if price_tag else 0.0
            stock = int(re.search(r"\d+", stock_tag.text).group()) if stock_tag else 0

            # Обновляем цену
            variant_update_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}.json"
            httpx.put(variant_update_url, headers=HEADERS, json={"variant": {"price": price}})

            log_product_to_csv(
                sku=variant.get("sku"),
                title=product["title"],
                variant=variant.get("title"),
                price=price,
                quantity=stock,
                tag="escentual"
            )

            # Обновляем количество
            inventory_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/inventory_levels/set.json"
            inventory_payload = {
                "location_id": LOCATION_ID,
                "inventory_item_id": inventory_item_id,
                "available": stock
            }
            httpx.post(inventory_url, headers=HEADERS, json=inventory_payload)

            print(f"✅ Обновлено: {handle} | Цена: {price} | Остаток: {stock}")
            updated_count += 1

    print(f"✅ Обновление завершено. Всего обновлено вариантов: {updated_count}")


def update_all_products_from_johnlewis():
    from playwright.sync_api import sync_playwright

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

    with sync_playwright() as p:
        browser = p.webkit.launch(headless=True)

        for product in all_products:
            if "johnlewis" not in product.get("tags", ""):
                continue

            for variant in product.get("variants", []):
                variant_id = variant["id"]
                inventory_item_id = variant["inventory_item_id"]
                variant_title = variant["title"]

                metafields_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}/metafields.json"
                response = httpx.get(metafields_url, headers=HEADERS)
                if response.status_code != 200:
                    print(f"⚠️ Не удалось получить метафилды для товара {product['id']}")
                    continue

                metafields = response.json().get("metafields", [])

                source_url = next(
                    (m["value"] for m in metafields if m["namespace"] == "global" and m["key"] == "source_url"),
                    None
                )

                if not source_url:
                    print(f"⚠️ Пропущен: не найден source_url для варианта {variant['id']}")
                    continue

                print(f"\n🌐 Ссылка для парсинга: {source_url}")
                time.sleep(5)

                try:
                    page = browser.new_page(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
                        extra_http_headers={"accept-language": "en-US,en;q=0.9"}
                    )

                    # retry goto
                    for attempt in range(2):
                        try:
                            page.goto(source_url, timeout=60000)
                            break
                        except Exception as e:
                            print(f"⚠️ Попытка {attempt + 1} — ошибка Playwright: {e}")
                            if attempt == 1:
                                raise e
                            time.sleep(3)

                    # Cookie
                    try:
                        page.locator('button:has-text("Allow all")').click(timeout=3000)
                        print("✅ Cookie-баннер закрыт")
                    except:
                        print("ℹ️ Cookie-баннер не появился")

                    # Stock
                    try:
                        page.wait_for_selector('[data-testid="product:basket:stock"]', timeout=10000)
                        stock_text = page.locator('[data-testid="product:basket:stock"]').inner_text()
                        match = re.search(r'\d+', stock_text)
                        if match:
                            stock = int(match.group())
                        elif "in stock" in stock_text.lower():
                            stock = 10
                        else:
                            stock = 0
                    except:
                        print("❌ Остаток не найден — устанавливаем 0")
                        stock = 0

                    # Цена — через ScraperAPI
                    try:
                        price_text = page.locator('[data-testid="product:basket:price"]').inner_text()
                        price = float(re.sub(r"[^\d.]", "", price_text)) if price_text else None
                    except:
                        price = None

                    print(f"✅ Остаток: {stock} | Цена: {price if price is not None else '—'}")

                    # Обновление цены
                    if price is not None:
                        variant_update_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/variants/{variant_id}.json"
                        httpx.put(variant_update_url, headers=HEADERS, json={"variant": {"price": price}})

                    # Обновление остатка
                    inventory_url = f"{SHOPIFY_STORE_URL}/admin/api/2024-01/inventory_levels/set.json"
                    inventory_payload = {
                        "location_id": LOCATION_ID,
                        "inventory_item_id": inventory_item_id,
                        "available": stock
                    }
                    httpx.post(inventory_url, headers=HEADERS, json=inventory_payload)

                    updated_count += 1
                    log_product_to_csv(
                        sku=variant.get("sku"),
                        title=product["title"],
                        variant=variant.get("title"),
                        price=price,
                        quantity=stock,
                        tag="johnlewis"
                    )

                    page.close()

                except Exception as e:
                    print(f"❌ Ошибка при обработке варианта: {e}")
                    continue

        browser.close()

    print(f"\n✅ Обновление завершено. Всего обновлено вариантов: {updated_count}")


def run_all_updates():
    print("🚀 Запуск последовательного обновления всех источников...")

    # Создаём временный файл
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    temp_filename = os.path.join(CSV_DIR, f"~temp_product_update_log.csv")
    log_product_to_csv.filename = temp_filename

    update_all_products_from_escentual()
    update_all_products_from_johnlewis()

    # После завершения — переименовываем временный файл
    final_filename = os.path.join(CSV_DIR, f"product_update_log_{timestamp}.csv")

    # Удаляем старые активные логи
    for f in os.listdir(CSV_DIR):
        if f.startswith("product_update_log_") and not f.startswith("~") and f.endswith(".csv"):
            os.remove(os.path.join(CSV_DIR, f))

    os.rename(temp_filename, final_filename)
    log_product_to_csv.filename = final_filename
    print(f"✅ CSV-файл обновлён и готов к скачиванию: {final_filename}")


def log_product_to_csv(sku: str, title: str, variant: str, price: float, quantity: int, tag: str):
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

    shipping_fee = 3.95 if price < 75 else 10.50
    variant_cleaned = "-" if not variant or variant.strip().lower() == "default title" else variant

    row = [
        sku or "-",
        title or "-",
        variant_cleaned,
        price,
        shipping_fee,
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
    return send_file(latest_file, as_attachment=True)


@app.route("/", methods=["GET", "POST"])
def index():
    status = ""
    if request.method == "POST":
        print("🚨 Форма отправлена")
        raw = request.form.get("links", "")
        print("📥 RAW:", repr(raw))
        links = [l.strip() for l in raw.splitlines() if l.startswith("https://")]
        grouped = defaultdict(list)

        for link in links:
            site = get_site_name(link)
            if site == "escentual":
                parsed = parse_escentual(link)
                title_key = parsed["title"].strip().lower()
                grouped[title_key].append(parsed)
            elif site == "johnlewis":
                handle, _, sku = extract_handle_variant_sku_from_url(link)
                grouped[f"{handle}-{sku}"].append(link)
            else:
                print(f"❌ Неизвестный сайт: {link}")
                status += f"<p style='color:red;'>❌ Link {link} — is an unsupported site</p>"
        for group_key, group_data in grouped.items():
            if isinstance(group_data[0], dict):  # escentual
                product_name = group_data[0].get("title", "Без названия")
                print(f"\n🔍 Обработка escentual: {product_name}")
                create_shopify_product(group_data, "escentual")
                status += f"<p>✅ {product_name} добавлен</p>"
            else:  # johnlewis
                site = get_site_name(group_data[0])
                print(f"\n🔍 Обработка группы {group_key} — сайт: {site}")
                parsed_list = []
                for url in group_data:
                    if site == "johnlewis":
                        parsed = parse_product(url)
                    else:
                        print(f"❌ Неизвестный сайт в ссылке: {url}")
                        continue
                    parsed_list.append(parsed)

                if parsed_list:
                    product_name = parsed_list[0].get("base_title") or parsed_list[0].get("title") or "Без названия"
                    create_shopify_product(parsed_list, site)
                    status += f"<p>✅ {product_name} added</p>"

    return render_template_string(r"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Loading Products</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                background-color: #f4f6f8;
                padding: 40px;
                color: #202223;
            }
            h1 {
                font-size: 24px;
                margin-bottom: 20px;
            }
            #editor {
                width: 100%;
                min-height: 200px;
                padding: 12px;
                font-size: 14px;
                border: 1px solid #ccc;
                border-radius: 8px;
                background: #fff;
                white-space: pre-wrap;
                outline: none;
            }
            #editor a {
                color: #2a72d4;
                text-decoration: underline;
            }
            button {
                margin-top: 12px;
                background-color: #5c6ac4;
                color: #fff;
                font-size: 14px;
                padding: 10px 20px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
            }
            button:hover {
                background-color: #3d4dad;
            }
            .status {
                margin-top: 24px;
                background: #fff;
                padding: 16px;
                border-radius: 8px;
                border: 1px solid #ddd;
            }
            p {
                margin: 0 0 8px;
            }
            #editor:empty:before {
              content: attr(data-placeholder);
              color: #aaa;
              pointer-events: none;
            }
        </style>
    </head>
    <body>
        <h1>Loading products</h1>
        <form method="post" id="mainForm">
            <div id="editor" contenteditable="true" data-placeholder="📝 Paste links one per line. Press Enter after each."></div>
            <textarea name="links" id="realInput" style="display:none;"></textarea>
            <button id="startButton" type="button">Start the process</button>
            <button type="button" onclick="window.open('/download_csv', '_blank')">📥 Download the latest CSV</button>
        </form>
        <div class="status">{{status|safe}}</div>

        <script>
            document.addEventListener("DOMContentLoaded", function () {
                const editor = document.getElementById('editor');
                const startButton = document.getElementById("startButton");
                const realInput = document.getElementById('realInput');
                const form = document.getElementById("mainForm");

                function updateLinks() {
                    const plain = editor.innerText;
                    const parts = plain.split(/(https?:\/\/[^\s,]+)/g);
                    let html = '';
                    for (let part of parts) {
                        if (part.match(/^https?:\/\//)) {
                            html += `<a href="${part}" target="_blank">${part}</a> `;
                        } else {
                            html += part.replace(/\n/g, '<br>');
                        }
                    }
                    editor.innerHTML = html.trim();
                    placeCaretAtEnd(editor);
                }

                function placeCaretAtEnd(el) {
                    el.focus();
                    if (typeof window.getSelection !== "undefined" && typeof document.createRange !== "undefined") {
                        const range = document.createRange();
                        range.selectNodeContents(el);
                        range.collapse(false);
                        const sel = window.getSelection();
                        sel.removeAllRanges();
                        sel.addRange(range);
                    }
                }

                editor.addEventListener('input', updateLinks);

                editor.addEventListener('paste', function(e) {
                    e.preventDefault();
                    const text = (e.clipboardData || window.clipboardData).getData('text');
                    document.execCommand("insertText", false, text);
                });

                startButton.addEventListener("click", function () {
                    console.log("🚀 Кнопка нажата");
                    const plainText = editor.innerText.trim();
                    const links = plainText.split(/\s+/).filter(l => l.startsWith("http"));
                    const invalidLinks = links.filter(link => {
                        return !link.includes("johnlewis.com") && !link.includes("escentual.com");
                    });

                    if (invalidLinks.length > 0) {
                        alert("❌ Unsupported links found:\n\n" + invalidLinks.join("\n") + "\n\nOnly links from johnlewis.com and escentual.com are allowed");
                        return;
                    }

                    realInput.value = plainText;
                    form.submit();
                });

            });
            </script>

    </body>
    </html>
    """, status=status)


executors = {
    'default': ThreadPoolExecutor(10)
}

scheduler = BackgroundScheduler(executors=executors)
scheduler.add_job(func=run_all_updates, trigger='interval', minutes=5)
scheduler.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False)
