# -*- coding: utf-8 -*-


import os
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructField, StructType, StringType

import numpy as np
import pandas as pd


################ METADATEN DEFINIEREN UND SPARKSESSION ERZEUGEN  UND DATEIEN EINLESEN   ################


pfad = os.path.abspath(".")
dateipfad = pfad +  "\\1_raw\\zensus2011\\daten\\"
datei_BEVOELKERUNG = "ZENSUS_BEVOELKERUNG.csv"
datei_GEBAEUDE = "ZENSUS_GEBAEUDE.csv"
datei_HAUSHALTE = "ZENSUS_HAUSHALTE.csv"


## Schemata zur automatisierten Typisierung aller Merkmale als String

schema_string_bev = "AGS_12;RS_Land;RS_RB_NUTS2;RS_Kreis;RS_VB;RS_Gem;Name;Reg_Hier;AEWZ;DEM_1.1;DEM_1.2;DEM_1.3;DEM_2.1;DEM_2.2;DEM_2.3;DEM_2.4;DEM_2.5;DEM_2.6;DEM_2.7;DEM_2.8;DEM_2.9;DEM_2.10;DEM_2.11;DEM_2.12;DEM_2.13;DEM_2.14;DEM_2.15;DEM_2.16;DEM_2.17;DEM_2.18;DEM_2.19;DEM_2.20;DEM_2.21;DEM_2.22;DEM_2.23;DEM_2.24;DEM_2.25;DEM_2.26;DEM_2.27;DEM_3.1;DEM_3.2;DEM_3.3;DEM_3.4;DEM_3.5;DEM_3.6;DEM_3.7;DEM_3.8;DEM_3.9;DEM_3.10;DEM_3.11;DEM_3.12;DEM_3.13;DEM_3.14;DEM_3.15;DEM_3.16;DEM_3.17;DEM_3.18;DEM_3.19;DEM_3.20;DEM_3.21;DEM_3.22;DEM_3.23;DEM_3.24;DEM_3.25;DEM_3.26;DEM_3.27;DEM_3.28;DEM_3.29;DEM_3.30;DEM_4.1;DEM_4.2;DEM_4.3;DEM_4.4;DEM_4.5;DEM_4.6;DEM_4.7;DEM_4.8;DEM_4.9;DEM_4.10;DEM_4.11;DEM_4.12;DEM_4.13;DEM_4.14;DEM_4.15;DEM_4.16;DEM_4.17;DEM_4.18;DEM_4.19;DEM_4.20;DEM_4.21;DEM_4.22;DEM_4.23;DEM_4.24;DEM_4.25;DEM_4.26;DEM_4.27;DEM_4.28;DEM_4.29;DEM_4.30;DEM_4.31;DEM_4.32;DEM_4.33;DEM_4.34;DEM_4.35;DEM_4.36;DEM_5.1;DEM_5.2;DEM_5.3;DEM_5.4;DEM_5.5;DEM_5.6;DEM_5.7;DEM_6.1;DEM_6.2;DEM_6.3;DEM_6.4;DEM_6.5;DEM_6.6;DEM_6.7;REL_1.1;REL_1.2;REL_1.3;REL_1.4;MIG_1.1;MIG_1.2;MIG_1.3;MIG_1.4;MIG_1.5;MIG_1.6;MIG_1.7;MIG_1.8;MIG_1.9;MIG_1.10;MIG_1.11;MIG_2.1;MIG_2.2;MIG_2.3;MIG_2.4;MIG_2.5;MIG_2.6;MIG_2.7;MIG_2.8;MIG_3.1;MIG_3.2;MIG_3.3;MIG_3.4;MIG_3.5;ERW_1.1;ERW_1.2;ERW_1.3;ERW_1.4;ERW_1.5;ERW_1.6;ERW_1.7;ERW_1.8;ERW_1.9;ERW_1.10;ERW_1.11;ERW_1.12;ERW_1.13;ERW_1.14;ERW_1.15;ERW_2.1;ERW_2.2;ERW_2.3;ERW_2.4;ERW_2.5;ERW_2.6;ERW_3.1;ERW_3.2;ERW_3.3;ERW_3.4;ERW_3.5;ERW_3.6;ERW_3.7;ERW_3.8;ERW_3.9;ERW_3.10;ERW_3.11;ERW_4.1;ERW_4.2;ERW_4.3;ERW_4.4;ERW_4.5;ERW_4.6;ERW_4.7;ERW_4.8;ERW_4.9;ERW_4.10;ERW_4.11;ERW_4.12;ERW_4.13;ERW_4.14;ERW_4.15;BIL_2.1;BIL_2.2;BIL_2.3;BIL_2.4;BIL_3.1;BIL_3.2;BIL_3.3;BIL_3.4;BIL_3.5;BIL_3.6;BIL_3.7;BIL_4.1;BIL_4.2;BIL_4.3;BIL_4.4;BIL_4.5;BIL_4.6;BIL_4.7;BIL_4.8;BIL_4.9;BIL_4.10;BIL_5.1;BIL_5.2;BIL_5.3;BIL_5.4;BIL_5.5;BIL_5.6;BIL_5.7;BIL_5.8"
schema_string_geb = "AGS_12;RS_Land;RS_RB_NUTS2;RS_Kreis;RS_VB;RS_Gem;Name;Reg_Hier;GEB_1.1;GEB_1.2;GEB_1.3;GEB_1.4;GEB_1.5;GEB_2.1;GEB_2.2;GEB_2.3;GEB_2.4;GEB_2.5;GEB_2.6;GEB_2.7;GEB_2.8;GEB_2.9;GEB_2.10;GEB_3.1;GEB_3.2;GEB_3.3;GEB_3.4;GEB_3.5;GEB_3.6;GEB_3.7;GEB_3.8;GEB_3.9;GEB_3.10;GEB_3.11;GEB_4.1;GEB_4.2;GEB_4.3;GEB_4.4;GEB_4.5;GEB_4.6;GEB_4.7;GEB_4.8;GEB_4.9;GEB_5.1;GEB_5.2;GEB_5.3;GEB_5.4;GEB_5.5;GEB_5.6;GEB_5.7;GEB_6.1;GEB_6.2;GEB_6.3;GEB_6.4;GEB_6.5;GEB_6.6;GEB_7.1;GEB_7.2;GEB_7.3;GEB_7.4;GEB_7.5;WHG_1.1;WHG_1.2;WHG_1.3;WHG_1.4;WHG_1.5;WHG_2.1;WHG_2.2;WHG_2.3;WHG_2.4;WHG_2.5;WHG_2.6;WHG_2.7;WHG_2.8;WHG_2.9;WHG_2.10;WHG_3.1;WHG_3.2;WHG_3.3;WHG_3.4;WHG_3.5;WHG_3.6;WHG_3.7;WHG_3.8;WHG_3.9;WHG_3.10;WHG_3.11;WHG_4.1;WHG_4.2;WHG_4.3;WHG_4.4;WHG_4.5;WHG_4.6;WHG_4.7;WHG_4.8;WHG_4.9;WHG_5.1;WHG_5.2;WHG_5.3;WHG_5.4;WHG_5.5;WHG_5.6;WHG_5.7;WHG_6.1;WHG_6.2;WHG_6.3;WHG_6.4;WHG_6.5;WHG_7.1;WHG_7.2;WHG_7.3;WHG_7.4;WHG_7.5;WHG_7.6;WHG_7.7;WHG_7.8;WHG_7.9;WHG_7.10;WHG_7.11;WHG_8.1;WHG_8.2;WHG_8.3;WHG_8.4;WHG_8.5;WHG_8.6;WHG_8.7;WHG_8.8;WHG_9.1;WHG_9.2;WHG_9.3;WHG_9.4;WHG_9.5"

fields_bev = [StructField(field_name.replace(".", "_"), StringType(), True) for field_name in schema_string_bev.split(";")]
schema_bev = StructType(fields_bev)

fields_geb = [StructField(field_name.replace(".", "_"), StringType(), True) for field_name in schema_string_geb.split(";")]
schema_geb = StructType(fields_geb)


schema_string_hh = "AGS_12;RS_Land;RS_RB_NUTS2;RS_Kreis;RS_VB;RS_Gem;Name;Reg_Hier;HH_1.1;HH_1.2;HH_1.3;HH_1.4;HH_1.5;HH_1.6;HH_2.1;HH_2.2;HH_2.3;HH_2.4;HH_2.5;HH_2.6;HH_2.7;HH_2.8;HH_3.1;HH_3.2;HH_3.3;HH_3.4;HH_3.5;HH_3.6;HH_3.7;HH_4.1;HH_4.2;HH_4.3;HH_4.4;FAM_1.1;FAM_1.2;FAM_1.3;FAM_1.4;FAM_2.1;FAM_2.2;FAM_2.3;FAM_2.4;FAM_2.5;FAM_2.6;FAM_3.1;FAM_3.2;FAM_3.3;FAM_3.4;FAM_3.5;FAM_3.6"

fields_hh = [StructField(field_name.replace(".", "_"), StringType(), True) for field_name in schema_string_hh.split(";")]

schema_hh = StructType(fields_hh)


spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("load and merge zensus")\
    .getOrCreate()



###### ANREICHERUNG: VERKNÜPFUNG DER DATENSÄTZE ######

df_hh = spark.read.csv(dateipfad + datei_HAUSHALTE, sep=";", header=True, encoding="ISO-8859-1", schema=schema_hh)
df_hh.createOrReplaceTempView("hh")
df_hh.select("AGS_12").show()
df_hh.count() #12544


## Für den JOIN müssen die Spalten umbenannt werden, welche in allen Datensätzen identisch vorliegen

df_bev = spark.read.csv(dateipfad + datei_BEVOELKERUNG, sep=";", header=True, encoding="ISO-8859-1",  schema=schema_bev)

## Da die Datensätze Merkmale besitzen die gleich bezeichnet werden, müssen diese mit prefixe versehen werden
prefix = "bev_"

df_bev = df_bev.withColumnRenamed("AGS_12", prefix + "AGS_12")\
                    .withColumnRenamed("RS_Land", prefix + "RS_Land")\
                    .withColumnRenamed("RS_RB_NUTS2", prefix + "RS_RB_NUTS2")\
                    .withColumnRenamed("RS_Kreis", prefix + "RS_Kreis")\
                    .withColumnRenamed("RS_VB", prefix + "RS_VB")\
                    .withColumnRenamed("RS_Gem", prefix + "RS_Gem")\
                    .withColumnRenamed("Name", prefix + "Name")\
                    .withColumnRenamed("Reg_Hier", prefix + "Reg_Hier")
                    
df_bev.createOrReplaceTempView("bev")
df_bev.select("bev_AGS_12").show()
df_bev.count() #12544



df_geb = spark.read.csv(dateipfad + datei_GEBAEUDE, sep=";", header=True, encoding="ISO-8859-1", schema=schema_geb)
 
prefix = "geb_"

df_geb = df_geb.withColumnRenamed("AGS_12", prefix + "AGS_12")\
                    .withColumnRenamed("RS_Land", prefix + "RS_Land")\
                    .withColumnRenamed("RS_RB_NUTS2", prefix + "RS_RB_NUTS2")\
                    .withColumnRenamed("RS_Kreis", prefix + "RS_Kreis")\
                    .withColumnRenamed("RS_VB", prefix + "RS_VB")\
                    .withColumnRenamed("RS_Gem", prefix + "RS_Gem")\
                    .withColumnRenamed("Name", prefix + "Name")\
                    .withColumnRenamed("Reg_Hier", prefix + "Reg_Hier")

df_geb.createOrReplaceTempView("gebaeude")
df_geb.select("geb_AGS_12").show()
df_geb.count() #12544

## JOIN

df_join = spark.sql("""
          SELECT hh.*, bev.*, gebaeude.* 
          FROM hh
              JOIN bev
                  ON hh.AGS_12 = bev.bev_AGS_12
              JOIN gebaeude
                  ON gebaeude.geb_AGS_12 = bev.bev_AGS_12   
          """)


## Tests 

df_join.count() #12544
df_join.select("AGS_12" ,"geb_AGS_12", "bev_AGS_12").show()
df_join.printSchema()

len(df_hh.columns) #49
len(df_bev.columns) #223
len(df_geb.columns) #132

len(df_hh.columns) + len(df_bev.columns) + len(df_geb.columns) #404

len(df_join.columns) #404


## Redundante Spalten löschen 


df_join = df_join.drop("bev_AGS_12", "bev_RS_Land", "bev_RS_RB_NUTS2", "bev_RS_Kreis", "bev_RS_VB", "bev_RS_Gem", "bev_Name", "bev_Reg_Hier", "geb_AGS_12", "geb_RS_Land", "geb_RS_RB_NUTS2", "geb_RS_Kreis", "geb_RS_VB", "geb_RS_Gem", "geb_Name", "geb_Reg_Hier")
df_join.printSchema()


###### BEREINIGUNG: STANDARDISIERUNG VON FEHLENDEN WERTEN UND ELIMINIERUNG DER MASKIERTEN WERTE (die unter dem  Geheimhaltungsverfahren stehen  ######


def ersetzung_funktion(wert):
    """ Ersetzt fehlende Werte mit None und demaskiert Werte die unter dem Geheimhaltungsverfahren geändert wurden """
    
    if(wert == None):
        wert_neu = wert

    elif(wert == ""):
        wert_neu = None
    
    elif(wert in ["-", "/"]):
        wert_neu = None

    elif("(" in wert):
        wert_neu = wert.replace("(", "").replace(")", "")
    
    elif("-" in wert):
        wert_neu = wert.replace("-", "")
        
    else:
        wert_neu = wert
    
    return wert_neu



def ersetzung_funktion(wert):
    """ Ersetzt fehlende Werte maskiert mit None """
    
    zeichen_fehlende_und_maskierte_werte = set("-/ ()")

    schnittmenge = set(wert).intersection(zeichen_fehlende_und_maskierte_werte)
    
    if(len(schnittmenge) > 0):
        wert_neu = None

    else:
        wert_neu = wert
    
    return wert_neu

print(ersetzung_funktion("89)"))

def mit_nullen_auffuellen(wert, anzahl_nullen):
    """ Füllt den jeweiligen Wert (hier Schlüsselattribute) mit der definierten Anzahl an Nullen auf """

    if(wert == None):
        wert_neu = "0"*int(anzahl_nullen) 
    else:
        wert_neu = wert.ljust(anzahl_nullen, "0")

    return wert_neu




mit_nullen_auffuellen_udf = udf(mit_nullen_auffuellen, StringType())



ersetzung_funktion_udf = udf(ersetzung_funktion, StringType())


## Für eine anschließende Analyse der fehlenden und maskierten Werte, wird das DataFrame vor der Bereinigung zwischengespeichert 
pd_df_join = df_join.toPandas()



## Zustand vor Bereinigung
df_join.select("HH_1_1", "DEM_1_1", "BIL_2_1", "WHG_1_1").show(50)



## Fehlende und maskierte Werte ersetzen
for spalte in df_join.columns[8:388]:
    df_join = df_join.withColumn(spalte, ersetzung_funktion_udf(df_join[spalte]))
    
    

## Zustand nach Bereinigung

df_join.select("HH_1_1", "DEM_1_1", "BIL_2_1", "WHG_1_1").show(50)


## Zustand vor Bereinigung


df_join.select("RS_Land", "RS_RB_NUTS2", "RS_Kreis", "RS_VB", "RS_Gem").show()

df_join = df_join.withColumn("AGS_12", mit_nullen_auffuellen_udf(col("AGS_12"), lit(12)))\
            .withColumn("RS_RB_NUTS2", mit_nullen_auffuellen_udf(col("RS_RB_NUTS2"), lit(1)))\
            .withColumn("RS_Kreis", mit_nullen_auffuellen_udf(col("RS_Kreis"), lit(2)))\
            .withColumn("RS_VB", mit_nullen_auffuellen_udf(col("RS_VB"), lit(4)))\
            .withColumn("RS_Gem", mit_nullen_auffuellen_udf(col("RS_Gem"), lit(3)))


## Zustand nach Bereinigung

df_join.select("RS_Land", "RS_RB_NUTS2", "RS_Kreis", "RS_VB", "RS_Gem").show()


## Neue Datei abspeichern
dateipfad_curated = pfad +  "\\2_curated\\daten\\"
datei_neu = "ZENSUS_BEVOELKERUNG_GEBAEUDE_HAUSHALTE.csv"

df_join.toPandas().to_csv(dateipfad_curated + datei_neu, sep=";", header=True, index=False, encoding="ISO-8859-1")

    

########## ANALYSE  ###########
### Bevor die maskierten Werte eliminiert werden, sollte die Anzahl dieser Werte pro Spalte festgehalten werden
### Im Zuge dessen wird ebenfalls der Anteil an fehlenden Werten festgehalten


df_analyse = pd.DataFrame(columns=["Spaltenname", "Anzahl Geheimhaltungen", "Relative Anzahl Geheimhaltungen", "Relative Anzahl Geheimhaltungen von vorhandenen Werten","Anzahl fehlende Werte", "Relative Anzahl fehlender Werte"])


gesamtzahl_daten = len(pd_df_join)
for spalte in pd_df_join.columns[8:388]:
    
    anzahl_fehlende_werte = len(pd_df_join[pd_df_join[spalte].isnull()])
    # "0" > "-1" - True
    anzahl_geheimhaltungen = len(pd_df_join[pd_df_join[spalte] < "0"])
    
    df_analyse = df_analyse.append({"Spaltenname":spalte,\
                           "Anzahl Geheimhaltungen" : anzahl_geheimhaltungen,\
                           "Relative Anzahl Geheimhaltungen" : round((anzahl_geheimhaltungen*100)/gesamtzahl_daten, 2),\
                           "Relative Anzahl Geheimhaltungen von vorhandenen Werten" : round((anzahl_geheimhaltungen*100)/(gesamtzahl_daten - anzahl_fehlende_werte), 2),\
                           "Anzahl fehlende Werte" : anzahl_fehlende_werte,\
                           "Relative Anzahl fehlender Werte" : round((anzahl_fehlende_werte*100)/gesamtzahl_daten, 2)},\
                            ignore_index=True)

df_analyse.dtypes
df_analyse.head()

df_analyse.sort_values(by="Anzahl Geheimhaltungen", ascending=False).head(20)
df_analyse.sort_values(by="Relative Anzahl Geheimhaltungen von vorhandenen Werten", ascending=False).head(20)
df_analyse.sort_values(by="Anzahl fehlende Werte", ascending=False).head(20)



df_analyse.to_csv(dateipfad + "ANALYSE.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")





