


import os
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType



pfad = os.path.abspath(".")
dateipfad = pfad +  "\\2_curated\\daten\\"
dateipfad_anwendungen = pfad +  "\\2_curated\\daten\\"

datei_OSM = "germany_20180915_nodes.parquet"
datei_geod = "GEOD_MIT_FILIALEN.csv"

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("geodaesiedaten (mit filialen) mit osm knotenpunkte verknüpfen")\
    .getOrCreate()


schema_geod = StructType([StructField("RS", StringType(), False),
                            StructField("AGS", StringType(), False),
                            StructField("RS_ALT", StringType(), True),
                            StructField("valid_geojson", StringType(), True),
                            StructField("feature_type", StringType(), True),
                            StructField("anzahl_filialen", IntegerType(), True),
                            StructField("filialen_ids", ArrayType(), True),
                            StructField("gesamtumsatz", FloatType(), True)])




regionen = spark.read.csv(dateipfad_anwendungen + datei_geod, sep=";", header=True, encoding="ISO-8859-1", schema=schema_geod)
regionen.show()


osm_nodes = spark.read.parquet(dateipfad + datei_OSM)
osm_nodes.show()
osm_nodes.printSchema()




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


## Nur die notwendigen geografischen Merkmale für die Verarbeitung verwenden
osm_nodes_koordinaten = osm_nodes[["ID", "LAT", "LONG"]]

## Mögliche Ansätze zur Perfomance-Verbesserung:
#
#   Partitionsfaktor erhöhen
#   Anzahl Executer erhöhen
#   Parquet File
#   DataFrame.cache()



regionen.persist(StorageLevel.MEMORY_ONLY)
regionen.cache()
regionen.rdd.getNumPartitions()



## OSM Punkte in Liste überführen
osm_punkte_liste = []
for node in osm_nodes_koordinaten.collect():
    point = Point(node["LONG"], node["LAT"])
    
    osm_punkte_liste.append(point)
    
    
## Regionsumrisse in Hauptspeicher laden
regionen.cache()


def nodes_zaehler(valid_geojson, nodes_liste):
    """Funktion zum Zählen von infrastrukturpunkten (nodes) in einer Region"""
    
    regionsumriss = konvertiere_string_zu_geo_objekt(valid_geojson)
    zaehler = 0
    
    for node in nodes_liste:
        if(regionsumriss.contains(node)):
            zaehler = zaehler + 1
                  
    return zaehler

## udf Funktion bilden
nodes_zaehler_udf = udf(nodes_zaehler, IntegerType())

## Liste mit Punkten broadcasten sodass diese jedem Worker zur Verfügung stehen
spark.broadcast(osm_punkte_liste)

## Durchführung der Transformation
regionen_angereichet = regionen.withColumn("osm_nodes_counter", nodes_zaehler_udf(col("valid_geojson"), osm_punkte_liste))


regionen_angereichet.show()

regionen_angereichet.toPandas().to_csv(dateipfad_anwendungen + "GEOD_MIT_FILIALEN_UND_NODES.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")



