# -*- coding: utf-8 -*-

import os
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf, col, concat
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

pfad = os.path.abspath(".")
dateipfad = pfad +  "\\1_raw\\destatis\\daten\\"

## Dateien mit Angaben auf Kreisebene
datei_EINKOMMEN = "DESTATIS_VERFUEGBARES_EINKOMMEN.csv"
datei_ARBEITSLOSIGKEIT = "DESTATIS_ARBEITSLOSIGKEIT.csv"
datei_INSOLVENZ = "DESTATIS_INSOLVENZ.csv"
datei_GEBIETSART = "DESTATIS_GEBIETSART.csv"

## Besitzt eine andere Struktur. Hier sind alle Verwaltungseinheiten vertreten
## Wird daher separat behandelt
datei_GEMEINDEVERZ = "DESTATIS_GEMEINDEVERZEICHNIS.csv"


spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("destatis: laden, verknüpfen und bereinigen")\
    .getOrCreate()


###### SCHEMADEFINITION. Zunächst wird alles als String abgebildet, da z.B. der RS mit einer
#    führenden 0 beginnen kann, oder fehlende Werte mit "." oder "-" identifziert werden (kein numerischer Datentyp).
    

schema_einkommen = StructType([StructField("JAHR", IntegerType(), False),
                            StructField("RS", StringType(), False),
                            StructField("GEBIET", StringType(), True),
                            StructField("EINK_JE_EINW", StringType(), True)])


schema_arbeitslosigkeit = StructType([StructField("JAHR",IntegerType(), False),
                            StructField("RS", StringType(), False),
                            StructField("GEBIET", StringType(), True),
                            StructField("ANTEIL_ARBEITSLOSE", StringType(), True),
                            StructField("ANTEIL_ARBEITSLOSE_ALTER_15_24", StringType(), True),
                            StructField("ANTEIL_ARBEITSLOSE_ALTER_55_64", StringType(), True),
                            StructField("ANTEIL_LANGZEITARBEITSLOSE", StringType(), True),
                            StructField("ANTEIL_ARBEITSLOSE_AUSLAENDER", StringType(), True)])



schema_gebietsart = StructType([StructField("JAHR",IntegerType(),False),
                            StructField("RS",StringType(),False),
                            StructField("GEBIET", StringType(),True),
                            StructField("ANTEIL_SIEDLUNG_VERKEHR", StringType(), True),
                            StructField("ANTEIL_ERHOLUNGSFLAECHE", StringType(), True),
                            StructField("ANTEIL_LANDWIRTSCHAFTSFLAECHE", StringType(), True),
                            StructField("ANTEIL_WALDFLAECHE", StringType(), True)])


schema_insolvenz = StructType([StructField("JAHR",IntegerType(), False),
                            StructField("RS",StringType(), False),
                            StructField("GEBIET", StringType(), True),
                            StructField("ANZAHL_BEANTR_VERFAHREN", StringType(), True),
                            StructField("ANZAHL_EROEFFNETE_VERFAHREN", StringType(), True),
                            StructField("ANZAHL_ABGEW_VERFAHREN", StringType(), True),
                            StructField("ANZAHL_BEANTR_VERFAHREN_MIT_SCHULDPLAN", StringType(), True),
                            StructField("ANZAHL_BESCHAEFTIGTE", StringType(), True),
                            StructField("FORDERUNGEN_TSD_EURO", StringType(), True)])



schema_gv = StructType([StructField("SATZART", IntegerType(), False),
                            StructField("TEXTKENNZEICHEN", IntegerType(), False),
                            StructField("RS_LAND", StringType(), False),
                            StructField("RS_REGBEZIRK", StringType(), False),
                            StructField("RS_KREIS", StringType(), False),
                            StructField("RS_GEMEINDEVB", StringType(), False),
                            StructField("RS_GEMEINDE", StringType(), False),
                            StructField("GEMEINDENAME", StringType(), False),
                            StructField("FLAECHE_IN_QKM", StringType(), True),
                            StructField("BEV_INSGESAMT", StringType(), True),
                            StructField("BEV_MAENNLICH", StringType(), True),
                            StructField("BEV_WEIBLICH", StringType(), True),
                            StructField("BEV_JE_QKM", StringType(), True),
                            StructField("PLZ", StringType(), True),
                            StructField("GEOG_MITTELP_LON", StringType(), True),
                            StructField("GEOG_MITTELP_LAT", StringType(), True),
                            StructField("BESIEDLUNGSSCHLUESSEL", IntegerType(), True),
                            StructField("BESIEDLUNGSSCHLUESSEL_BEZ", StringType(), True)])



df_gv = spark.read.csv(dateipfad + datei_GEMEINDEVERZ, sep=";", header=False, encoding="ISO-8859-1", schema=schema_gv)
df_gv.show()



df_einkommen = spark.read.csv(dateipfad + datei_EINKOMMEN, sep=";", header=True, encoding="ISO-8859-1", schema=schema_einkommen, ignoreLeadingWhiteSpace=True)
df_einkommen.createOrReplaceTempView("einkommen")

df_arbeitslosigkeit = spark.read.csv(dateipfad + datei_ARBEITSLOSIGKEIT, sep=";", header=True, encoding="ISO-8859-1", schema=schema_arbeitslosigkeit, ignoreLeadingWhiteSpace=True)
df_arbeitslosigkeit.createOrReplaceTempView("arbeitslosigkeit")


df_insolvenz = spark.read.csv(dateipfad + datei_INSOLVENZ, sep=";", header=True, encoding="ISO-8859-1", schema=schema_insolvenz, ignoreLeadingWhiteSpace=True)
df_insolvenz.createOrReplaceTempView("insolvenz")


df_gebietsart = spark.read.csv(dateipfad + datei_GEBIETSART, sep=";", header=True, encoding="ISO-8859-1", schema=schema_gebietsart, ignoreLeadingWhiteSpace=True)
df_gebietsart.createOrReplaceTempView("gebietsart")

df_einkommen.printSchema()
df_arbeitslosigkeit.printSchema()
df_insolvenz.printSchema()
df_gebietsart.printSchema()


df_einkommen.show()
df_arbeitslosigkeit.select("RS", "JAHR").show()
df_insolvenz.select("RS", "JAHR").show()
df_gebietsart.show()



spark.sql("SELECT COUNT(RS) FROM einkommen").show()  #3156
spark.sql("SELECT COUNT(RS) FROM arbeitslosigkeit").show() #3682
spark.sql("SELECT COUNT(RS) FROM insolvenz").show() #4208
spark.sql("SELECT COUNT(RS) FROM gebietsart").show() #3156


spark.sql("SELECT COUNT(DISTINCT(RS)) FROM einkommen").show()  #526
spark.sql("SELECT COUNT(DISTINCT(RS)) FROM arbeitslosigkeit").show() #526
spark.sql("SELECT COUNT(DISTINCT(RS)) FROM insolvenz").show() #526
spark.sql("SELECT COUNT(DISTINCT(RS)) FROM gebietsart").show() #526




spark.sql("SELECT COUNT(*), JAHR FROM einkommen GROUP BY JAHR").show()  #526 pro JAHR
spark.sql("SELECT COUNT(*), JAHR FROM arbeitslosigkeit GROUP BY JAHR").show()  #526 pro JAHR
spark.sql("SELECT COUNT(*), JAHR FROM insolvenz GROUP BY JAHR").show()  #526 pro JAHR
spark.sql("SELECT COUNT(*), JAHR FROM gebietsart GROUP BY JAHR").show()  #526 pro JAHR


spark.sql("SELECT JAHR FROM einkommen GROUP BY JAHR").show() #Messraum: 2010 bis 2015
spark.sql("SELECT JAHR FROM arbeitslosigkeit GROUP BY JAHR").show()  #Messraum: 2010 bis 2016
spark.sql("SELECT JAHR FROM insolvenz GROUP BY JAHR").show()  #Messraum: 2000, 2010 bis 2016
spark.sql("SELECT JAHR FROM gebietsart GROUP BY JAHR").show()  #Messraum: 2010 bis 2015




###### ANREICHERUNG: VERKNÜPFUNG DER DATENSÄTZE ######

df_join = spark.sql("""
                    SELECT 
                    einkommen.JAHR, einkommen.RS, einkommen.GEBIET, einkommen.EINK_JE_EINW,
                    arbeitslosigkeit.ANTEIL_ARBEITSLOSE, arbeitslosigkeit.ANTEIL_ARBEITSLOSE_ALTER_15_24, arbeitslosigkeit.ANTEIL_ARBEITSLOSE_ALTER_55_64, arbeitslosigkeit.ANTEIL_LANGZEITARBEITSLOSE, arbeitslosigkeit.ANTEIL_ARBEITSLOSE_AUSLAENDER,
                    insolvenz.ANZAHL_BEANTR_VERFAHREN, insolvenz.ANZAHL_EROEFFNETE_VERFAHREN, insolvenz.ANZAHL_ABGEW_VERFAHREN, insolvenz.ANZAHL_BEANTR_VERFAHREN_MIT_SCHULDPLAN, insolvenz.ANZAHL_BESCHAEFTIGTE, insolvenz.FORDERUNGEN_TSD_EURO,
                    gebietsart.ANTEIL_SIEDLUNG_VERKEHR, gebietsart.ANTEIL_ERHOLUNGSFLAECHE, gebietsart.ANTEIL_LANDWIRTSCHAFTSFLAECHE, gebietsart.ANTEIL_WALDFLAECHE
                    FROM einkommen
                    JOIN arbeitslosigkeit
                        ON einkommen.RS = arbeitslosigkeit.RS
                    JOIN insolvenz
                        ON arbeitslosigkeit.RS = insolvenz.RS
                    JOIN gebietsart
                        ON insolvenz.RS = gebietsart.RS
                    WHERE 
                        einkommen.JAHR = arbeitslosigkeit.JAHR 
                        AND
                        arbeitslosigkeit.JAHR = insolvenz.JAHR 
                        AND
                        insolvenz.JAHR = gebietsart.JAHR
                    """)

df_join.count()

df_join.printSchema()


df_join_vor_bereinigung = df_join.toPandas()


###### BEREINIGUNG: STANDARDISIERUNG VON DEZIMALKOMMA UND FEHLENDEN WERTEN  ######


### Funktion, um fehlende Werte durch None (Python Null) zu ersetzen ###
ersetze_mit_null_udf = udf(lambda value: None if value in ["" ," ", "-", "."] else value, StringType())

### Funktion, um Dezimalkomma durch Dezimalpunkt zu ersetzen. Besonderheit: Es muss getestet werden
### ob der Wert ein String ist. Dies soll ausschließen, dass kein Nullwert vorliegt, auf dem value.replace() zu einem Fehler führt

ersetze_dezimalkomma_udf = udf(lambda value: value.replace(',','.') if isinstance(value, str) else value, StringType())


spalten_mit_dezimalkomma = ["EINK_JE_EINW",\
                           "ANTEIL_ARBEITSLOSE",\
                           "ANTEIL_ARBEITSLOSE_ALTER_15_24",\
                           "ANTEIL_ARBEITSLOSE_ALTER_55_64",\
                           "ANTEIL_LANGZEITARBEITSLOSE",\
                           "ANTEIL_ARBEITSLOSE_AUSLAENDER",\
                           "ANTEIL_SIEDLUNG_VERKEHR",\
                           "ANTEIL_ERHOLUNGSFLAECHE",\
                           "ANTEIL_LANDWIRTSCHAFTSFLAECHE",\
                           "ANTEIL_WALDFLAECHE"]



for spalte in df_join.columns:
    
    df_join = df_join.withColumn(spalte, ersetze_mit_null_udf(df_join[spalte]))
    
    if(spalte in spalten_mit_dezimalkomma):
        df_join = df_join.withColumn(spalte, ersetze_dezimalkomma_udf(df_join[spalte]))


### Test ob fehlende Werte ersetzt worden sind ###
df_join_vor_bereinigung.count()
df_join.toPandas().count()

### Test ob Dezimalkomma durch Punkt ersetzt worden ist
df_join.select(spalten_mit_dezimalkomma).show()


### Den RS mit Nullen auffüllen sodass dieser 12 stellig wird (für bessere Zusammenführbarkeit mit anderen Datensätzen)
rs_auffuellen_udf = udf(lambda x: x.ljust(12, "0"))
df_join = df_join.withColumn("RS", rs_auffuellen_udf(df_join["RS"]))

df_join.select("RS").show()

### Funktioniert nicht
#df_join.write.save(dateipfad + "datei.csv", format="csv")

### Datei abspeichern ###

dateipfad_curated = pfad +  "\\2_curated\\daten\\"
dateiname = "DESTATIS_INSOLVENZ_ARBEITSLOSIGKEIT_EINKOMMEN_GEBIETSART.csv"
df_join.toPandas().to_csv(dateipfad_curated + dateiname, sep=";", header=True, index=False, encoding="ISO-8859-1")

    


#### GEMEINDEVERZEICHNIS

df_gv.printSchema()

df_gv.select("BEV_INSGESAMT").show()

df_gv = df_gv.drop("SATZART", "TEXTKENNZEICHEN")



## Fehlende Werte überprüfen
df_gv.toPandas().count() # Datensätze voll besetzt

## Der RS liegt in den einzelnen Fragmenten vor. Diese werden zunächst zusammengeführt um ein zusammenhängendes Schlüsselattribut zu erzeugen
df_gv = df_gv.withColumn("RS", concat(col("RS_LAND"), col("RS_REGBEZIRK"), col("RS_KREIS"), col("RS_GEMEINDEVB"), col("RS_GEMEINDE")))
df_gv.select("RS").show()



spalten_mit_leerzeichen = ["BEV_INSGESAMT", "BEV_MAENNLICH", "BEV_WEIBLICH", "BEV_JE_QKM"]
spalten_mit_dezimalkomma = ["FLAECHE_IN_QKM", "GEOG_MITTELP_LON", "GEOG_MITTELP_LAT"]


df_gv.select(spalten_mit_dezimalkomma).show()
for spalte in spalten_mit_dezimalkomma:
    
    df_gv = df_gv.withColumn(spalte, ersetze_dezimalkomma_udf(df_gv[spalte]))
    
# Test
df_gv.select(spalten_mit_dezimalkomma).show()
    

## Leerzeichen eliminieren
ersetze_leerzeichen_udf = udf(lambda value: value.replace(" ","") if isinstance(value, str) else value, StringType())


df_gv.select(spalten_mit_leerzeichen).show()
for spalte in spalten_mit_leerzeichen:
    
    df_gv = df_gv.withColumn(spalte, ersetze_leerzeichen_udf(df_gv[spalte]))
    
# Test
df_gv.select(spalten_mit_leerzeichen).show()



dateiname = "DESTATIS_GEMEINDEVERZEICHNIS.csv"
df_gv.toPandas().to_csv(dateipfad_curated + dateiname, sep=";", header=True, index=False, encoding="ISO-8859-1")

