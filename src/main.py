from extract import extract_data
from transform import transform_data, validate_data
from load import load_to_postgres

def main():

    # EXTRACT
    df1, df2, df3 = extract_data()

    # TRANSFORM
    df_staging, df_core, df_gold = transform_data(df1, df2, df3)

    # VALIDATE
    validate_data(df_staging, df_core, df_gold)

    # LOAD
    load_to_postgres(df_staging, "products_1", "staging")
    load_to_postgres(df_core, "products_2", "core")
    load_to_postgres(df_gold, "products_classified", "gold")


if __name__ == "__main__":
    main()
