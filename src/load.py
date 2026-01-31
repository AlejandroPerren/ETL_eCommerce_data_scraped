import os
from sqlalchemy import create_engine

def get_engine():
    """
    Creates and returns a SQLAlchemy PostgreSQL engine.
    """
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}",
        pool_pre_ping=True
    )


def load_to_postgres(df, table_name, schema):
    """
    Loads a dataframe into PostgreSQL.
    """
    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )
