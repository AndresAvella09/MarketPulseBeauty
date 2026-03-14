from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import logging

logging.basicConfig(filename='logs/scraping.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        page.goto("https://www.sephora.com/brands-list", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_selector("a[href*='/brand/']", timeout=15000)
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, 'html.parser')

    brand_link_lst = []
    main_box = soup.find_all(attrs={"data-comp": "BrandsList BrandsList BaseComponent "})[0]
    for brand in main_box.find_all('li'):
        if brand.a and brand.a.get('href'):
            brand_link_lst.append("https://www.sephora.com" +
                                  brand.a.attrs['href'] + "/all?pageSize=300")

    with open('data/raw/links/brand_link.txt', 'w') as f:
        for item in brand_link_lst:
            f.write(f"{item}\n")

    logging.info(f'Got All Brand Links! There are {len(brand_link_lst)} brands in total.')
except Exception as e:
    logging.error(f'Error in scrape_brand_links.py: {e}')