import pandas as pd
import csv
from io import StringIO


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check si el dataframe está vacío
    if df.empty:
        return False
    
    # Check si la Primary Key es única 
    if not df["played_at"].is_unique:
        raise Exception("A value from played_at is not unique")
    
    # Check si existen valores nulos
    if df.isnull().values.any():
        raise Exception("A value in df is null")
        
    return True


# Función tomada de la documentacion de Pandas
# Alternative to_sql() *method* for DBs that support COPY FROM
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)