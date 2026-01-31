import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

def extract_data():
    files = [
        "productsfull.csv",
        "productsfull2.csv",
        "productsclassified.csv",
    ]

    for file in files:
        file_path = RAW_DIR / file
        if not file_path.exists():
            raise FileNotFoundError(f"Missing data file: {file_path}")

        pd.read_csv(file_path)

        print(f"[EXTRACT] OK -> {file_path}")
