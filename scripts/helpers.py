from io import StringIO
import csv
import os
import json
import base64
from requests import post, get


env = os.environ

# Spotify settings
CLIENT_ID = env['SPOTIFY_CLIENT_ID']
CLIENT_SECRET = env['SPOTIFY_CLIENT_SECRET']

def get_token():
    auth_string = CLIENT_ID + ":" + CLIENT_SECRET
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }
    result = post(url, headers=headers, data=data)
    if result.status_code == 200:
        json_result = json.loads(result.content)
        token = json_result["access_token"]
    else:
        print("Error al generar el token")
        raise Exception("Error al generar el token")

    return token

def get_auth_header(token):
    return {
        "Authorization": "Bearer " + token
    }


def search_for_artist(token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"
    
    query_url = url + query
    result = get(query_url, headers=headers)
    if result.status_code == 200:
        json_result = json.loads(result.content)["artists"]["items"]
    else:
        print("Error al extraer datos de la Web API Spotify")
        raise Exception("Error al extraer datos de la Web API Spotify")

    if len(json_result) == 0:
        print("No existe artista con ese nombre...")
        return None
    
    return json_result[0]


def get_artist_top_tracks(token, id_artist, country):
    # Get Spotify catalog information about an artist's top tracks by country.
    url = f"https://api.spotify.com/v1/artists/{id_artist}/top-tracks"
    headers = get_auth_header(token)
    query = f"?market={country}"
    
    query_url = url + query
    result = get(query_url, headers=headers)
    if result.status_code == 200:
        json_result = json.loads(result.content)["tracks"]
    else:
        print("Error al extraer canciones del artista")
        raise Exception("Error al extraer canciones del artista")

    return json_result


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