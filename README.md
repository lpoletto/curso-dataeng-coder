# ETL-Spotify 

![Arquitectura](etl-arquitectura.png)

ETL que extrae las canciones reproducidas del día anterior de un usuario de Spotify

## Pasos
1. Registrarse en la API de [Spotify](https://developer.spotify.com/documentation/web-api/tutorials/getting-started).

2. Crea el entorno virtual suguiendo las siguentes instrucciones en un terminal:

### Windows
```sh
python -m venv venv
venv\Scripts\activate
python -m pip install -r requirements.txt
```

### Linux
```sh
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```
