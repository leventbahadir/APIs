import requests
from bs4 import BeautifulSoup
import csv
import time
import re

base_url = "https://www.esteelauder.com"
listing_url = base_url + "/products/26403/product-catalog/skincare/by-concern/pores"

headers = {
    "User-Agent": "LeventBot/1.0 (+https://github.com/levent/esteelauder-scraper)"
}

request_delay = 1.5  

def fetch_product_links():
    response = requests.get(listing_url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    links = []
    for a in soup.select("a[href*='/product/']"):
        href = a.get("href")
        if href and not href.startswith("#"):
            full_link = base_url + href if href.startswith("/") else href
            links.append(full_link)
    return list(dict.fromkeys(links))[:100]  

def scrape_product_page(url):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Name
    name_tag = soup.find("h1")
    name = name_tag.get_text(strip=True) if name_tag else "N/A"

    desc_tag = soup.select_one(".product-description, .product-details__description")
    description = desc_tag.get_text(strip=True) if desc_tag else "N/A"

    sku = "N/A"
    img_tags = soup.find_all("img", src=True)
    for img in img_tags:
        match = re.search(r'el_sku_([A-Z0-9]+)_', img['src'])
        if match:
            sku = match.group(1)
            break


    variants = []
    variant_tags = soup.select("ul li.option, .shade-name, select option")
    for tag in variant_tags:
        text = tag.get_text(strip=True)
        if text:
            variants.append(text)
    variants_text = ", ".join(dict.fromkeys(variants)) if variants else "N/A"

    return {
        "Name": name,
        "SKU": sku,
        "Variants/Colors": variants_text,
        "Description": description,
        "URL": url
    }

def main():
    print("🔍 Fetching product listing...")
    product_links = fetch_product_links()
    print(f"🔗 Found {len(product_links)} product links.")

    scraped_data = []
    for i, link in enumerate(product_links, 1):
        print(f"[{i}/{len(product_links)}] Scraping: {link}")
        try:
            product_data = scrape_product_page(link)
            scraped_data.append(product_data)
        except Exception as e:
            print(f"Error scraping {link}: {e}")
        time.sleep(request_delay)

    if scraped_data:
        with open("estee_lauder_pores_100.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=scraped_data[0].keys())
            writer.writeheader()
            writer.writerows(scraped_data)
        print(f"\n Data saved to 'estee_lauder_pores_100.csv'.")
    else:
        print("No data scraped.")

if __name__ == "__main__":
    main()
