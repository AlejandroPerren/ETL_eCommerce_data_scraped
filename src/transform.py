import ast
import json
import pandas as pd

# ============================================================
# Helpers
# ============================================================

def parse_string_list(raw_value):
    """
    Converts strings that represent Python lists into real lists.
    Always returns a list.
    """
    if raw_value in (None, "", "[]"):
        return []

    if isinstance(raw_value, list):
        return raw_value

    if isinstance(raw_value, str) and raw_value.strip().startswith("["):
        try:
            value = ast.literal_eval(raw_value)
            return value if isinstance(value, list) else []
        except Exception:
            return []

    return []


def list_to_json_or_null(value):
    """
    Converts list values into JSON strings.
    Returns None if the list is empty.
    """
    if isinstance(value, list):
        return json.dumps(value) if len(value) > 0 else None
    return value


def replace_empty_with_null(value):
    """
    Replaces empty values and NaN with None.
    Scalar-safe (lists are handled before this step).
    """
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, str) and value.strip() == "":
        return None

    return value


def normalize_text_columns(dataframe, text_columns):
    """
    Normalizes text columns: lowercase, trim and 'none' to null.
    """
    for column_name in text_columns:
        if column_name in dataframe.columns:
            dataframe[column_name] = (
                dataframe[column_name]
                .astype(str)
                .str.lower()
                .str.strip()
                .replace("none", None)
            )
    return dataframe


def normalize_itemid_column(dataframe):
    """
    Normalizes itemid so it is always a single scalar value.
    """
    def extract_itemid_value(value):
        if isinstance(value, list):
            return value[0] if value else None

        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned.startswith("[") and cleaned.endswith("]"):
                cleaned = cleaned.strip("[]").replace("'", "").strip()
            return cleaned if cleaned else None

        return None

    dataframe["itemid"] = dataframe["itemid"].apply(extract_itemid_value)
    return dataframe


# ============================================================
# Main transform
# ============================================================

def transform_dataframes(
    staging_dataframe,
    core_dataframe,
    gold_dataframe
):
    """
    Applies cleaning and normalization to staging, core and gold dataframes.
    Ensures no list values reach the load step.
    """

    staging_dataframe = staging_dataframe.copy()
    core_dataframe = core_dataframe.copy()
    gold_dataframe = gold_dataframe.copy()

    list_like_columns = ["colorname", "desc2", "fit", "fitinfo"]

    # 1. Parse list-like columns
    for df in (staging_dataframe, core_dataframe, gold_dataframe):
        for col in list_like_columns:
            if col in df.columns:
                df[col] = df[col].apply(parse_string_list)

    # 2. Convert lists to JSON strings or NULL
    for df in (staging_dataframe, core_dataframe, gold_dataframe):
        for col in list_like_columns:
            if col in df.columns:
                df[col] = df[col].apply(list_to_json_or_null)

    # 3. Replace empty / NaN values
    staging_dataframe = staging_dataframe.map(replace_empty_with_null)
    core_dataframe = core_dataframe.map(replace_empty_with_null)
    gold_dataframe = gold_dataframe.map(replace_empty_with_null)

    # 4. Normalize itemid
    staging_dataframe = normalize_itemid_column(staging_dataframe)
    core_dataframe = normalize_itemid_column(core_dataframe)
    gold_dataframe = normalize_itemid_column(gold_dataframe)

    # 5. Normalize text fields
    text_columns = ["name", "description"]
    staging_dataframe = normalize_text_columns(staging_dataframe, text_columns)
    core_dataframe = normalize_text_columns(core_dataframe, text_columns)

    # 6. Defensive filtering
    staging_dataframe = staging_dataframe[staging_dataframe["itemid"].notna()]
    core_dataframe = core_dataframe[core_dataframe["itemid"].notna()]
    gold_dataframe = gold_dataframe[gold_dataframe["itemid"].notna()]

    # 7. Core must be unique
    core_dataframe = core_dataframe.drop_duplicates(subset=["itemid"])

    # 8. Final Airflow / SQL safety net
    staging_dataframe = staging_dataframe.where(pd.notnull(staging_dataframe), None)
    core_dataframe = core_dataframe.where(pd.notnull(core_dataframe), None)
    gold_dataframe = gold_dataframe.where(pd.notnull(gold_dataframe), None)

    return staging_dataframe, core_dataframe, gold_dataframe

# ============================================================
# Validations
# ============================================================

def validate_dataframes(
    staging_dataframe,
    core_dataframe,
    gold_dataframe
):
    """
    Validates basic data integrity rules.
    """

    # STAGING
    assert staging_dataframe["itemid"].notna().all(), "STAGING: itemid is null"

    # CORE
    assert core_dataframe["itemid"].notna().all(), "CORE: itemid is null"
    assert core_dataframe["itemid"].is_unique, "CORE: duplicated itemid"
    assert core_dataframe["name"].notna().all(), "CORE: name is null"

    # GOLD
    assert gold_dataframe["itemid"].notna().all(), "GOLD: itemid is null"

