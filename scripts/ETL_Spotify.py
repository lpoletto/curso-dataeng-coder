# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla popular_songs

import pandas as pd
from datetime import datetime, timedelta
from os import environ as env
from pyspark.sql.functions import concat, col, lit, when, expr, to_date
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType

from commons import ETL_Spark
from helpers import get_token, search_for_artist, get_artist_top_tracks


class ETL_Spotify(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-09" # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la Web API Spotify...")
        COUNTRY_CODES=["AR", "BR", "US", "MX"]
        ARTISTS_LIST=["Daft Punk", "Soda Stereo", "Arctic Monkeys", "The Strokes"]

        today = datetime.now()
        id_songs = []
        song_names = []
        artist_names = []
        album_names = []
        popularity_list = []
        duration_ms_list = []
        song_links = []
        country_code_list = []
        timestamps = []

        # Obtenemos el token de Spotify
        token = get_token()

        # Buscamos el id de cada artista
        id_artists = []
        songs = []
        
        for artist in ARTISTS_LIST:
            result = search_for_artist(token, artist_name=artist)
            id_artists.append(result["id"])

        # Obtenemos las canciones de cada artista   
        for country in COUNTRY_CODES:
            for id in id_artists:
                songs = get_artist_top_tracks(token, id_artist=id, country=country)
                # Guardamos la data en listas   
                for song in songs:
                    id_songs.append(song["id"])
                    song_names.append(song["name"])
                    artist_names.append(song["artists"][0]["name"])
                    album_names.append(song["album"]["name"])
                    popularity_list.append(song["popularity"])
                    duration_ms_list.append(song["duration_ms"])
                    song_links.append(song["external_urls"]["spotify"])
                    country_code_list.append(country)
                    timestamps.append(today)

        # Creamos un diccionario de listas
        song_dict = {
            "id_song": id_songs,
            "song_name": song_names,
            "artist": artist_names,
            "album": album_names,
            "popularity": popularity_list,
            "duration_ms": duration_ms_list,
            "song_link": song_links,
            "country_code": country_code_list,
            "timestamp_": timestamps
        }

        # Creamos el DataFrame
        df = pd.DataFrame(song_dict)
        return df
    

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")
        my_schema = StructType([ StructField("id_song", StringType(), False)\
                      ,StructField("song_name", StringType(), False)\
                      ,StructField("artist", StringType(), False)\
                      ,StructField("album", StringType(), False)\
                      ,StructField("popularity", IntegerType(), False)\
                      ,StructField("duration_ms", IntegerType(), False)\
                      ,StructField("song_link", StringType(), False)\
                      ,StructField("country_code", StringType(), False)\
                      ,StructField("timestamp_", TimestampType(), False)\
                    ])

        # Crea el DataFrame con los nombres de las columnas especificadas
        df = self.spark.createDataFrame(df_original, schema=my_schema)

        # Crear una columna personalizada para la clave primaria
        df = df.withColumn("alternate_key", concat(col("id_song"), col("country_code")))
        
        # Comprobar si el DataFrame esta vacio
        if df.rdd.isEmpty():
            raise Exception("The DataFrame is empty")

        # Comprobar si la Primary Key es unica
        if not df.select("alternate_key").distinct().count() == df.count():
            raise Exception("A value from id is not unique")
        
        df.printSchema()
        df.show()

        return df
    

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        # df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.popular_songs") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Spotify()
    etl.run()