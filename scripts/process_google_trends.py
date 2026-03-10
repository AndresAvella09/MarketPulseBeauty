from pathlib import Path
from src.processing.datasetConstructor import DatasetConstructor

RAW_DIR = Path("data/raw/google_trends")
OUT_DIR = Path("data/processed/google_trends")

for csv_path in RAW_DIR.glob("*.csv"):
    try:
        dc = DatasetConstructor(csv_path)
        dc.separe_google_dataset(output_dir=str(OUT_DIR))
        print("Processed", csv_path.name)
    except Exception as e:
        print("Failed", csv_path.name, ":", e)