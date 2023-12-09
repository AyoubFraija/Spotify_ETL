import spotipy
import spotipy.util as util
import json
import datetime
import requests
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine

CLIENT_ID = "11caaf2609e04e39954094125829c14b"
CLIENT_SECRET = "6eaa6866263141eba5eff437207951e9"
USERNAME = "31bbniimaigzzipnulb4uj5k5n2y"
SCOPE = "user-read-recently-played"
REDIRECT_URI = "http://localhost:8888/callback"

#Obtention du token access 
token = util.prompt_for_user_token(USERNAME, SCOPE, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
#Obtention des données
if token:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=25&after=1484811043508", headers=headers)
    data = r.json()
    
else:
    print("Unable to get token for", USERNAME)
#Nettoyage des données et selection des colonnes importantes
songs=[]
artiste=[]
date_ecoute=[]
heure_ecoute=[]
album=[]

for chanson in data["items"]:
    songs.append(chanson["track"]["name"])
    artiste.append(chanson["track"]["album"]["artists"][0]["name"])
    date_ecoute.append(chanson["played_at"][0:10])
    heure_ecoute.append(chanson["played_at"][12:])
    album.append(chanson["track"]["album"]["name"])
#Création d'un dictionnaire qui ne contient que les données voulues
song_dict={
    "nom_chanson" : songs ,
    "nom_artiste" : artiste ,
    "date d'écoute" : date_ecoute ,
    "heure d'écoute" : heure_ecoute ,
    "nom_album" : album
}
#Conversion du dico en DataFrame
donnée=pd.DataFrame(song_dict, columns = ["nom_chanson","nom_artiste","date d'écoute","heure d'écoute","nom_album"])


def validation_data(df: pd.DataFrame) -> bool :
    #Checker si la data est vide
    if df.empty:
        print("Aucune chanson n'a été écouté !")
        return False
    
    #Checker si la contrainte de la clé primaire a été violée
    if pd.Series(df["heure d'écoute"]).is_unique:
        pass
    else:
        raise Exception("La contrainte de clé primaire a été violée")
    
    #Checker si la valuer NUll existe dans la dataframe
    if df.isnull().values.any():
        raise Exception("Il existe un champ NUll dans la data")
    
    return True

#On verifie si la data obtenue est valide
if validation_data(donnée):
    print("Data Valide ! Vous pouvez procéder à l'étape du LOADING")

print(donnée)

#On se connect à la base de donnée MySQL
db=mysql.connector.connect(host="localhost",user="root",password="03112002",database="Spotify_ETL")
c=db.cursor()
sql_query="""
CREATE TABLE IF NOT EXISTS chanson_ecoutee(
    nom_chanson VARCHAR(200),
    nom_artiste VARCHAR(200),
    date_ecoute VARCHAR(200),
    heure_ecoute VARChAR(200),
    nom_album VARCHAR(200),
    PRIMARY KEY(heure_ecoute)
)
"""
c.execute(sql_query)
db.commit()
engine = create_engine("mysql+mysqlconnector://root:03112002@localhost/Spotify_ETL")
donnée.to_sql(con=engine, name='chanson_ecoutee', if_exists='replace', index=False)
db.close()
print("Connexion à la base de donnée finie !")









