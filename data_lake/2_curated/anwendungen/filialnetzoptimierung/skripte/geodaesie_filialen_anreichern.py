# -*- coding: utf-8 -*-



import os
import pandas as pd
import numpy as np

pfad = os.path.abspath(".")
dateipfad = pfad +  "\\2_curated\\daten\\"

datei_FILIALEN = "FILIALEN_MIT_GEOLOKATION.csv"
datei_GEOD = "GEOD_KREIS_GEM_GRENZEN.csv"


schema_filialen = {"ID": "int", "Strassenname": "str", "PLZ": "str",
               "Region": "str", "Anzahl_Mitarbeiter": "int", 
               "Verkaufsflaeche": "float", "Jahresumsatz": "float",
                "latitude": "float", "longitude": "float"}

filialen = pd.read_csv(dateipfad + datei_FILIALEN, sep=";", dtype=schema_filialen, encoding="ISO-8859-1")
filialen.dtypes
len(filialen) #3348

        
regionen = pd.read_csv(dateipfad + datei_GEOD, sep=";", dtype=object, encoding="ISO-8859-1")
regionen.dtypes
len(regionen) #11864


# Nur die für das Matching relevanten Attribute extrahieren
regionen = regionen[["AGS", "RS", "RS_ALT", "valid_geojson", "feature_type"]]
filialen = filialen[["ID", "latitude", "longitude"]]

region_filiale_matchingliste = []
filiale_region_matchingliste = []



from shapely.geometry import Point, Polygon, MultiPolygon, shape
def konvertiere_string_zu_geo_objekt(geo_objekt_str):
    """ Konvertiert einen String mit einen Geo-Objekt in eine geometrische Repräsentation in Python (Polygon, Point, Multipolygon) """
     # Hier konvertiert eval() eine String-Respräsentation einer List in eine Liste 
    
    # eval() ist eine builtin-Funktion und wertet einen String aus und interpretiert diesen in bekannte Typen
    # Hier wird der String in die komplexe Listenstruktur des GeoJSON überführt
    geo_objekt = eval(geo_objekt_str)
    
    if(geo_objekt["type"] == "Polygon"):
        
        geo_objekt_koordinaten = geo_objekt["coordinates"][0]
        
        neues_polygon = []
        
        for punkt in geo_objekt_koordinaten:
            
            neues_polygon.append(tuple(punkt))
            
        
        valides_geo_objekt = Polygon(neues_polygon) 

        
    elif(geo_objekt["type"] == "MultiPolygon"):
        
        multipolygon_koordinaten = geo_objekt["coordinates"]
        
        neues_polygon = []
        
        for polygon in multipolygon_koordinaten:
            
            neues_polygon_punkte = []
            
            for point in polygon[0]:
                
                neues_polygon_punkte.append(tuple(point))
    
            neues_polygon.append(Polygon(neues_polygon_punkte))
            
        valides_geo_objekt = MultiPolygon(neues_polygon) 
    else:
        print("feature type nicht erkannt: " + geo_objekt)
        
    return valides_geo_objekt




#######################################################################################
############ Für jede Region die darin befindlichen Filialen finden #######################
#######################################################################################



import time
start = time.time()

## Zwei Möglichkeiten zur Iteration:
#itertupels Vorteil: Schneller und originale Datentypen
#iterrows Nachtteil: Series Objekte werden erzeugt und dadurch Typveränderung (int -> int64)

for region in regionen.itertuples():
    
    region_umriss = konvertiere_string_zu_geo_objekt(region.valid_geojson)
        
    filialen_liste = []
        
    for filiale in filialen.itertuples():
            #filiale --> tuple: (index,(ID, lat, lon))
        filiale_koordinaten = Point(filiale.longitude, filiale.latitude)
            
        if(region_umriss.contains(filiale_koordinaten)):
            #print("match: " + region.RS + " und " + str(branch_location[1]["ID"]))
            filialen_liste.append(int(filiale.ID))
            filiale_region_matchingliste.append({"filiale_id" : filiale.ID, "region_rs" : region.RS})
            
    if(len(filialen_liste)>0):
        print(len(filialen_liste)) 
        region_filiale_matchingliste.append({"AGS" : region.AGS, "RS" : region.RS, "RS_ALT" : region.RS_ALT,
                                         "filialen_ids" : filialen_liste, "anzahl_filialen" : len(filialen_liste)})
        
end = time.time()
print((end - start) / 60) #12 min
len(region_filiale_matchingliste) #2412 
len(filiale_region_matchingliste) #6783

regionen_mit_filialen = pd.DataFrame(region_filiale_matchingliste)      
regionen_mit_filialen.dtypes
regionen_mit_filialen.head()


join = regionen.set_index("RS").join(regionen_mit_filialen.drop(columns=["AGS", "RS_ALT"]).set_index("RS"), how="left")

join = join.reset_index()

join.dtypes

len(join) #12195
len(regionen) #11975

len(join["RS"].unique()) #11864
len(regionen["RS"].unique()) #11864


### Gesamtumsatz und durchschnittlicher Umsatz für jede Region errechnen


def summiere_filalumsaetze(filialen_ids):
    
    summierter_umsatz = 0
    
    if (type(filialen_ids) == list):
    
        for filiale_id in filialen_ids:
            
            filialumsatz = filialen[filialen["ID"] == filiale_id]["Jahresumsatz"].values[0]
            summierter_umsatz = summierter_umsatz + filialumsatz
        
    else:
         summierter_umsatz = None
        
    return summierter_umsatz



join["gesamtumsatz"] = join["filialen_ids"].apply(lambda x: summiere_filalumsaetze(x))

join["durchschnittlicher_umsatz"] = join["gesamtumsatz"]/join["anzahl_filialen"]

join[["durchschnittlicher_umsatz", "RS"]].head()

dateipfad_anwendungen = pfad +  "\\2_curated\\anwendungen\\filialnetzoptimierung\\daten\\"

join.to_csv(dateipfad_anwendungen + "GEOD_MIT_FILALEN.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")







