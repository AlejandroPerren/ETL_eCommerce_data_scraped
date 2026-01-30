import ast

# ============================================================
# Helpers
# ============================================================

def parse_list(x):
    if x in [None, "", "[]"]:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str) and x.startswith('['):
        try:
            return ast.literal_eval(x)
        except:
            return []
    return []


def clean_empty(x):
    if x in ["", None]:
        return None
    return x


def normalize_text(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.lower()
                .str.strip()
                .replace("none", None)
            )
    return df


def clean_itemid(df):
    def extract_itemid(x):
        if isinstance(x, list):
            return x[0] if x else None
        if isinstance(x, str):
            x = x.strip()
            if x.startswith("[") and x.endswith("]"):
                x = x.strip("[]").replace("'", "").strip()
                return x if x else None
        return x

    df["itemid"] = df["itemid"].apply(extract_itemid)
    return df


# ============================================================
# Transform principal
# ============================================================

def transform_data(df1, df2, df3):

    df1 = df1.copy()
    df2 = df2.copy()
    df3 = df3.copy()

    list_columns = ["colorname", "desc2"]

    for col in list_columns:
        if col in df1.columns:
            df1[col] = df1[col].apply(parse_list)
        if col in df2.columns:
            df2[col] = df2[col].apply(parse_list)
        if col in df3.columns:
            df3[col] = df3[col].apply(parse_list)

    df1 = df1.map(clean_empty)
    df2 = df2.map(clean_empty)
    df3 = df3.map(clean_empty)

    df1 = clean_itemid(df1)
    df2 = clean_itemid(df2)
    df3 = clean_itemid(df3)

    text_columns = ["name", "description", "fit", "fitinfo"]
    df1 = normalize_text(df1, text_columns)
    df2 = normalize_text(df2, text_columns)

    # filtro defensivo
    df1 = df1[df1["itemid"].notna()]
    df2 = df2[df2["itemid"].notna()]
    df3 = df3[df3["itemid"].notna()]

    # CORE debe ser único
    df2 = df2.drop_duplicates(subset=["itemid"])

    return df1, df2, df3


# ============================================================
# Validaciones
# ============================================================

def validate_data(df_staging, df_core, df_gold):

    assert df_staging["itemid"].notna().all()

    assert df_core["itemid"].notna().all()
    assert df_core["itemid"].is_unique
    assert df_core["name"].notna().all()

    assert df_gold["itemid"].notna().all()
