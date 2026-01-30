import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data"

def extract_data():
    df_full_products = pd.read_csv(RAW_DIR / "productsfull.csv")
    df_full_products2 = pd.read_csv(RAW_DIR / "productsfull2.csv")
    df_products_classifield = pd.read_csv(RAW_DIR / "productsclassified.csv")

    return (
        df_full_products,
        df_full_products2,
        df_products_classifield
    )
