import ast

# ============================================================
# Helpers
# ============================================================

def parse_string_list(raw_value):
    """
    Convierte strings que representan listas en listas reales.
    """
    if raw_value in [None, "", "[]"]:
        return []

    if isinstance(raw_value, list):
        return raw_value

    if isinstance(raw_value, str) and raw_value.startswith('['):
        try:
            return ast.literal_eval(raw_value)
        except Exception:
            return []

    return []


def replace_empty_with_null(cell_value):
    """
    Reemplaza valores vacíos por None.
    """
    if cell_value in ["", None]:
        return None
    return cell_value


def normalize_text_columns(dataframe, text_columns):
    """
    Normaliza columnas de texto: lowercase, trim y 'none' a null.
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
    Normaliza la columna itemid para que sea un valor único simple.
    """
    def extract_itemid_value(itemid_value):
        if isinstance(itemid_value, list):
            return itemid_value[0] if itemid_value else None

        if isinstance(itemid_value, str):
            cleaned_value = itemid_value.strip()
            if cleaned_value.startswith("[") and cleaned_value.endswith("]"):
                cleaned_value = (
                    cleaned_value
                    .strip("[]")
                    .replace("'", "")
                    .strip()
                )
                return cleaned_value if cleaned_value else None
            return cleaned_value

        return None

    dataframe["itemid"] = dataframe["itemid"].apply(extract_itemid_value)
    return dataframe


# ============================================================
# Transform principal
# ============================================================

def transform_dataframes(
    staging_dataframe,
    core_dataframe,
    gold_dataframe
):
    """
    Aplica limpieza y normalización a staging, core y gold.
    """

    staging_dataframe = staging_dataframe.copy()
    core_dataframe = core_dataframe.copy()
    gold_dataframe = gold_dataframe.copy()

    list_like_columns = ["colorname", "desc2"]

    for column_name in list_like_columns:
        if column_name in staging_dataframe.columns:
            staging_dataframe[column_name] = staging_dataframe[column_name].apply(parse_string_list)

        if column_name in core_dataframe.columns:
            core_dataframe[column_name] = core_dataframe[column_name].apply(parse_string_list)

        if column_name in gold_dataframe.columns:
            gold_dataframe[column_name] = gold_dataframe[column_name].apply(parse_string_list)

    staging_dataframe = staging_dataframe.map(replace_empty_with_null)
    core_dataframe = core_dataframe.map(replace_empty_with_null)
    gold_dataframe = gold_dataframe.map(replace_empty_with_null)

    staging_dataframe = normalize_itemid_column(staging_dataframe)
    core_dataframe = normalize_itemid_column(core_dataframe)
    gold_dataframe = normalize_itemid_column(gold_dataframe)

    text_columns = ["name", "description", "fit", "fitinfo"]

    staging_dataframe = normalize_text_columns(staging_dataframe, text_columns)
    core_dataframe = normalize_text_columns(core_dataframe, text_columns)

    # Filtro defensivo
    staging_dataframe = staging_dataframe[staging_dataframe["itemid"].notna()]
    core_dataframe = core_dataframe[core_dataframe["itemid"].notna()]
    gold_dataframe = gold_dataframe[gold_dataframe["itemid"].notna()]

    # CORE debe ser único
    core_dataframe = core_dataframe.drop_duplicates(subset=["itemid"])

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
    Valida reglas básicas de integridad de datos.
    """

    # STAGING
    assert staging_dataframe["itemid"].notna().all(), "STAGING: itemid nulo"

    # CORE
    assert core_dataframe["itemid"].notna().all(), "CORE: itemid nulo"
    assert core_dataframe["itemid"].is_unique, "CORE: itemid duplicado"
    assert core_dataframe["name"].notna().all(), "CORE: name nulo"

    # GOLD
    assert gold_dataframe["itemid"].notna().all(), "GOLD: itemid nulo"
