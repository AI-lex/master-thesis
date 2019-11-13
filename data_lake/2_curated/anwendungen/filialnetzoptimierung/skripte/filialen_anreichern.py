
#coding: utf-8

 # Geo-Enrichment via Google API
 
 ### Vorgehen:
#
# Datei mit Unternehmensstandorten (via Webscraping "https://www.meinprospekt.de/filialen/lidl"). Die Standorte sind derzeit nur über Adressen lokalisiert und sollen mit konkreten Koordinaten angereichert werden
# SparkSession initiieren und Datei in ein DataFrame laden
# Adressen extrahieren und in ein Request einbauen, welcher dann an die Google API geschickt wird. Das zurückgegebene JSON Objekt anschließend parsen
# Die gewonnen Geo-Informationen pro Filiale in ein DataFrame schreiben und anschließend mit dem ürsprünglichen DataFrame (ohne Geo-Koordinaten) zusammenführen
# Das neu gebildete DataFrame mit angereicherten Geo-Informationen in eine Datei überführen und ins HDFS schreiben 
# 
 
 ### Google API Limits:
# 
# Ohne API Key (kann über ein Google Konto generiert werden) ist eine Anfrage (API HTTP- Request) pro Sekunde möglich. Das Limit beschränkt sich ingesamt auf 2500 Requests pro Tag
# Mit API Key sind 50 Request pro Sekunde möglich, jedoch auch hier auf 2500 Anfragen gedeckelt.
# Kostenpflichte Variante: $0.50 pro 1000 Anfragen. Unbeschränkte Requests pro Sekunde.


import pandas as pd
import os

import requests
import time


pfad = os.path.abspath(".")
dateipfad = pfad +  "\\1_raw\\intern\\daten\\"
datei_FILIALEN = "filialen.csv"


schema = {"ID": "int", "Strassenname": "str", "PLZ": "str", "Region": "str"} 

filialen = pd.read_csv(dateipfad + datei_FILIALEN, sep=";", dtype=schema, encoding="UTF-8")

filialen.dtypes
filialen.head()


def get_koordinaten(filial_id, filial_strassenname, filial_plz, filial_region, api_key=None):
    """ Extrahiert die geografischen Koordinaten einer Standortaddresse über die Google API"""
    
    adresse_string = filial_region + " " + str(filial_plz) + " " + filial_strassenname
    google_api_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(adresse_string)
    
    wartezeit = 2
    
    koordinaten = {}
    
    if api_key is not None:
        google_api_url = google_api_url + "&key={}".format(api_key)
        #wait_time = 0.1
    
    #Google API ansteuern mit Parametern
    response = requests.get(google_api_url)
    #Response in JSON umwandeln um es zu parsen
    response = response.json()
    print(response)
    koordinaten_objekt = response["results"][0]
    latitude = koordinaten_objekt.get("geometry").get("location").get("lat")
    longitude = koordinaten_objekt.get("geometry").get("location").get("lng")
    
    koordinaten.update({"ID": filial_id})
    koordinaten.update({"latitude": latitude})
    koordinaten.update({"longitude": longitude})
    
    time.sleep(wartezeit)
    
    return koordinaten




#Beispiel Request: https://maps.googleapis.com/maps/api/geocode/json?address={Thale%206502%20Musestieg%205}

koordinaten_liste = []
apikey = None 

for filiale in filialen:
    koordinaten_liste.append(get_koordinaten(filiale["ID"], filiale["Strassenname"], filiale["PLZ"], filiale["Region"], apikey))


koordinaten_liste
len(koordinaten_liste)

filialen_koordinaten = pd.DataFrame(koordinaten_liste)

filialen_koordinaten.dtypes
filialen_koordinaten.head(20)


### Datenbestände zusammenführen und in eine Datei überführen


filialen_angereichert = filialen.set_index("ID").join(filialen_koordinaten.set_index("ID"), how="inner")

filialen_angereichert.dtypes
filialen_angereichert.head()

## Neue Datei abspeichern
dateipfad_curated = pfad +  "\\2_curated\\daten\\"
datei_neu = "INTERN_FILIALEN.csv"
filialen_angereichert.to_csv(dateipfad_curated + datei_neu, sep=";", header=True, index=False, encoding="ISO-8859-1")



##### TESTS #####

#### Schema des Google API Response Objekts (JSON)

json = {
   "results" : [
      {
         "address_components" : [
            {
               "long_name" : "5",
               "short_name" : "5",
               "types" : [ "street_number" ]
            },
            {
               "long_name" : "Musestieg",
               "short_name" : "Musestieg",
               "types" : [ "route" ]
            },
            {
               "long_name" : "Thale",
               "short_name" : "Thale",
               "types" : [ "locality", "political" ]
            },
            {
               "long_name" : "Harz",
               "short_name" : "Harz",
               "types" : [ "administrative_area_level_3", "political" ]
            },
            {
               "long_name" : "Sachsen-Anhalt",
               "short_name" : "SA",
               "types" : [ "administrative_area_level_1", "political" ]
            },
            {
               "long_name" : "Deutschland",
               "short_name" : "DE",
               "types" : [ "country", "political" ]
            },
            {
               "long_name" : "06502",
               "short_name" : "06502",
               "types" : [ "postal_code" ]
            }
         ],
         "formatted_address" : "Musestieg 5, 06502 Thale, Deutschland",
         "geometry" : {
            "location" : {
               "lat" : 51.75044760000001,
               "lng" : 11.0445755
            },
            "location_type" : "ROOFTOP",
            "viewport" : {
               "northeast" : {
                  "lat" : 51.75179658029151,
                  "lng" : 11.0459244802915
               },
               "southwest" : {
                  "lat" : 51.74909861970851,
                  "lng" : 11.0432265197085
               }
            }
         },
         "partial_match" : "true",
         "place_id" : "ChIJVXTbvQ6kpUcR8ghJIwJpAqk",
         "types" : [ "street_address" ]
      }
   ],
   "status" : "OK"
}



json["results"][0].get("geometry").get("location").get("lat")

json["results"][0].get("geometry").get("location").get("lng")






