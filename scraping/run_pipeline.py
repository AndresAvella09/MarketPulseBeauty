import subprocess
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None,
                    help="Limit items when running in development mode")
args = parser.parse_args()


def run_step(script, output_file, use_limit=False):
    if os.path.exists(output_file):
        print(f"Saltando {script} (ya existe {output_file})")
        return

    command = ["python", script]

    # Solo pasa --limit si:
    # 1) El usuario lo indicó
    # 2) El script lo soporta
    if args.limit and use_limit:
        command.extend(["--limit", str(args.limit)])

    print(f"Ejecutando {' '.join(command)}")
    subprocess.run(command, check=True)


# 🔹 PASOS DEL PIPELINE

run_step("scraping/scraper/scrape_brand_links.py",
         "data/txt/brand_link.txt",
         use_limit=True)

run_step("scraping/scraper/scrape_product_links.py",
         "data/txt/product_links.txt",
         use_limit=True)

run_step("scraping/scraper/scrape_product_info.py",
         "data/csv/pd_info.csv",
         use_limit=True)

run_step("scraping/scraper/scrape_reviews.py",
         "data/json/scrape/result.json",
         use_limit=True)

run_step("scraping/scraper/parse_reviews.py",
         "data/csv/review_data.csv",
         use_limit=False)  # este normalmente no necesita limit