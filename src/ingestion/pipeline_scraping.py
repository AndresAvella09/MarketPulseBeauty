import subprocess
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, default=None,
                    help="Limit items when running in development mode")
args = parser.parse_args()

# ✅ Resolve the scraper directory relative to this file
SCRAPER_DIR = os.path.join(os.path.dirname(__file__), "scraper")
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def run_step(script, output_file, use_limit=False):
    abs_output = os.path.join(PROJECT_ROOT, output_file)

    if os.path.exists(abs_output):
        print(f"Saltando {script} (ya existe {output_file})")
        return

    script_path = os.path.join(SCRAPER_DIR, script)
    command = ["python", script_path]

    if args.limit and use_limit:
        command.extend(["--limit", str(args.limit)])

    print(f"Ejecutando {' '.join(command)}")
    subprocess.run(command, check=True)


# 🔹 PASOS DEL PIPELINE

run_step("scrape_brand_links.py",
         "data/raw/txt/brand_link.txt",
         use_limit=True)

run_step("scrape_product_links.py",
         "data/raw/txt/product_links.txt",
         use_limit=True)

run_step("scrape_reviews.py",
         "data/raw/json/scraper_result.json",
         use_limit=True)

run_step("scrape_product_info.py",
         "data/raw/csv/pd_info.csv",
         use_limit=True)

run_step("parse_reviews.py",
         "data/raw/csv/review_data.csv",
         use_limit=False)