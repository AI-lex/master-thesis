# -*- coding: utf-8 -*-


import os
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

from pyspark.sql.functions import col


spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("zensus destatis verknüpfen und fehlende Werte schätzen")\
    .getOrCreate()

pfad = os.path.abspath(".")
dateipfad = pfad +  "\\2_curated\\daten\\"
dateipfad_anwendungen = pfad +  "\\2_curated\\anwendungen\\filialnetzoptimierung\\daten\\"

datei_DESTATIS = "DESTATIS_INSOLVENZ_ARBEITSLOSIGKEIT_EINKOMMEN_GEBIETSART_HIERACHIE_GESCHAETZT_2011.csv"
datei_DESTATIS_GV = "DESTATIS_GEMEINDEVERZEICHNIS.csv"
datei_ZENSUS = "ZENSUS_BEVOELKERUNG_GEBAEUDE_HAUSHALTE.csv"



schema_destatis_gv = StructType([StructField("RS_LAND", StringType(), False),
                            StructField("RS_REGBEZIRK", StringType(), False),
                            StructField("RS_KREIS", StringType(), False),
                            StructField("RS_GEMEINDEVB", StringType(), False),
                            StructField("RS_GEMEINDE", StringType(), False),
                            StructField("GEMEINDENAME", StringType(), False),
                            StructField("FLAECHE_IN_QKM", FloatType(), True),
                            StructField("BEV_INSGESAMT", IntegerType(), True),
                            StructField("BEV_MAENNLICH", IntegerType(), True),
                            StructField("BEV_WEIBLICH", IntegerType(), True),
                            StructField("BEV_JE_QKM", IntegerType(), True),
                            StructField("PLZ", StringType(), True),
                            StructField("GEOG_MITTELP_LON", FloatType(), True),
                            StructField("GEOG_MITTELP_LAT", FloatType(), True),
                            StructField("BESIEDLUNGSSCHLUESSEL", IntegerType(), True),
                            StructField("BESIEDLUNGSSCHLUESSEL_BEZ", StringType(), True),
                            StructField("RS", StringType(), False)])


schema_destatis = StructType([StructField("RS", StringType(), False),
                            StructField("GEBIET", StringType(), True),
                            StructField("EINK_JE_EINW", FloatType(), True),
                            StructField("ANTEIL_ARBEITSLOSE", FloatType(), True),
                            StructField("ANTEIL_ARBEITSLOSE_ALTER_15_24", FloatType(), True),
                            StructField("ANTEIL_ARBEITSLOSE_ALTER_55_64", FloatType(), True),
                            StructField("ANTEIL_LANGZEITARBEITSLOSE", FloatType(), True),
                            StructField("ANTEIL_ARBEITSLOSE_AUSLAENDER", FloatType(), True),
                            StructField("ANZAHL_BEANTR_VERFAHREN", FloatType(), True),
                            StructField("ANZAHL_EROEFFNETE_VERFAHREN", FloatType(), True),
                            StructField("ANZAHL_ABGEW_VERFAHREN", FloatType(), True),
                            StructField("ANZAHL_BEANTR_VERFAHREN_MIT_SCHULDPLAN", FloatType(), True),
                            StructField("ANZAHL_BESCHAEFTIGTE", FloatType(), True),
                            StructField("FORDERUNGEN_TSD_EURO", FloatType(), True),
                            StructField("ANTEIL_SIEDLUNG_VERKEHR", FloatType(), True),
                            StructField("ANTEIL_ERHOLUNGSFLAECHE", FloatType(), True),
                            StructField("ANTEIL_LANDWIRTSCHAFTSFLAECHE", FloatType(), True),
                            StructField("ANTEIL_WALDFLAECHE", FloatType(), True)])



schema_string_zensus = "AGS_12;RS_Land;RS_RB_NUTS2;RS_Kreis;RS_VB;RS_Gem;Name;Reg_Hier;HH_1_1;HH_1_2;HH_1_3;HH_1_4;HH_1_5;HH_1_6;HH_2_1;HH_2_2;HH_2_3;HH_2_4;HH_2_5;HH_2_6;HH_2_7;HH_2_8;HH_3_1;HH_3_2;HH_3_3;HH_3_4;HH_3_5;HH_3_6;HH_3_7;HH_4_1;HH_4_2;HH_4_3;HH_4_4;FAM_1_1;FAM_1_2;FAM_1_3;FAM_1_4;FAM_2_1;FAM_2_2;FAM_2_3;FAM_2_4;FAM_2_5;FAM_2_6;FAM_3_1;FAM_3_2;FAM_3_3;FAM_3_4;FAM_3_5;FAM_3_6;AEWZ;DEM_1_1;DEM_1_2;DEM_1_3;DEM_2_1;DEM_2_2;DEM_2_3;DEM_2_4;DEM_2_5;DEM_2_6;DEM_2_7;DEM_2_8;DEM_2_9;DEM_2_10;DEM_2_11;DEM_2_12;DEM_2_13;DEM_2_14;DEM_2_15;DEM_2_16;DEM_2_17;DEM_2_18;DEM_2_19;DEM_2_20;DEM_2_21;DEM_2_22;DEM_2_23;DEM_2_24;DEM_2_25;DEM_2_26;DEM_2_27;DEM_3_1;DEM_3_2;DEM_3_3;DEM_3_4;DEM_3_5;DEM_3_6;DEM_3_7;DEM_3_8;DEM_3_9;DEM_3_10;DEM_3_11;DEM_3_12;DEM_3_13;DEM_3_14;DEM_3_15;DEM_3_16;DEM_3_17;DEM_3_18;DEM_3_19;DEM_3_20;DEM_3_21;DEM_3_22;DEM_3_23;DEM_3_24;DEM_3_25;DEM_3_26;DEM_3_27;DEM_3_28;DEM_3_29;DEM_3_30;DEM_4_1;DEM_4_2;DEM_4_3;DEM_4_4;DEM_4_5;DEM_4_6;DEM_4_7;DEM_4_8;DEM_4_9;DEM_4_10;DEM_4_11;DEM_4_12;DEM_4_13;DEM_4_14;DEM_4_15;DEM_4_16;DEM_4_17;DEM_4_18;DEM_4_19;DEM_4_20;DEM_4_21;DEM_4_22;DEM_4_23;DEM_4_24;DEM_4_25;DEM_4_26;DEM_4_27;DEM_4_28;DEM_4_29;DEM_4_30;DEM_4_31;DEM_4_32;DEM_4_33;DEM_4_34;DEM_4_35;DEM_4_36;DEM_5_1;DEM_5_2;DEM_5_3;DEM_5_4;DEM_5_5;DEM_5_6;DEM_5_7;DEM_6_1;DEM_6_2;DEM_6_3;DEM_6_4;DEM_6_5;DEM_6_6;DEM_6_7;REL_1_1;REL_1_2;REL_1_3;REL_1_4;MIG_1_1;MIG_1_2;MIG_1_3;MIG_1_4;MIG_1_5;MIG_1_6;MIG_1_7;MIG_1_8;MIG_1_9;MIG_1_10;MIG_1_11;MIG_2_1;MIG_2_2;MIG_2_3;MIG_2_4;MIG_2_5;MIG_2_6;MIG_2_7;MIG_2_8;MIG_3_1;MIG_3_2;MIG_3_3;MIG_3_4;MIG_3_5;ERW_1_1;ERW_1_2;ERW_1_3;ERW_1_4;ERW_1_5;ERW_1_6;ERW_1_7;ERW_1_8;ERW_1_9;ERW_1_10;ERW_1_11;ERW_1_12;ERW_1_13;ERW_1_14;ERW_1_15;ERW_2_1;ERW_2_2;ERW_2_3;ERW_2_4;ERW_2_5;ERW_2_6;ERW_3_1;ERW_3_2;ERW_3_3;ERW_3_4;ERW_3_5;ERW_3_6;ERW_3_7;ERW_3_8;ERW_3_9;ERW_3_10;ERW_3_11;ERW_4_1;ERW_4_2;ERW_4_3;ERW_4_4;ERW_4_5;ERW_4_6;ERW_4_7;ERW_4_8;ERW_4_9;ERW_4_10;ERW_4_11;ERW_4_12;ERW_4_13;ERW_4_14;ERW_4_15;BIL_2_1;BIL_2_2;BIL_2_3;BIL_2_4;BIL_3_1;BIL_3_2;BIL_3_3;BIL_3_4;BIL_3_5;BIL_3_6;BIL_3_7;BIL_4_1;BIL_4_2;BIL_4_3;BIL_4_4;BIL_4_5;BIL_4_6;BIL_4_7;BIL_4_8;BIL_4_9;BIL_4_10;BIL_5_1;BIL_5_2;BIL_5_3;BIL_5_4;BIL_5_5;BIL_5_6;BIL_5_7;BIL_5_8;GEB_1_1;GEB_1_2;GEB_1_3;GEB_1_4;GEB_1_5;GEB_2_1;GEB_2_2;GEB_2_3;GEB_2_4;GEB_2_5;GEB_2_6;GEB_2_7;GEB_2_8;GEB_2_9;GEB_2_10;GEB_3_1;GEB_3_2;GEB_3_3;GEB_3_4;GEB_3_5;GEB_3_6;GEB_3_7;GEB_3_8;GEB_3_9;GEB_3_10;GEB_3_11;GEB_4_1;GEB_4_2;GEB_4_3;GEB_4_4;GEB_4_5;GEB_4_6;GEB_4_7;GEB_4_8;GEB_4_9;GEB_5_1;GEB_5_2;GEB_5_3;GEB_5_4;GEB_5_5;GEB_5_6;GEB_5_7;GEB_6_1;GEB_6_2;GEB_6_3;GEB_6_4;GEB_6_5;GEB_6_6;GEB_7_1;GEB_7_2;GEB_7_3;GEB_7_4;GEB_7_5;WHG_1_1;WHG_1_2;WHG_1_3;WHG_1_4;WHG_1_5;WHG_2_1;WHG_2_2;WHG_2_3;WHG_2_4;WHG_2_5;WHG_2_6;WHG_2_7;WHG_2_8;WHG_2_9;WHG_2_10;WHG_3_1;WHG_3_2;WHG_3_3;WHG_3_4;WHG_3_5;WHG_3_6;WHG_3_7;WHG_3_8;WHG_3_9;WHG_3_10;WHG_3_11;WHG_4_1;WHG_4_2;WHG_4_3;WHG_4_4;WHG_4_5;WHG_4_6;WHG_4_7;WHG_4_8;WHG_4_9;WHG_5_1;WHG_5_2;WHG_5_3;WHG_5_4;WHG_5_5;WHG_5_6;WHG_5_7;WHG_6_1;WHG_6_2;WHG_6_3;WHG_6_4;WHG_6_5;WHG_7_1;WHG_7_2;WHG_7_3;WHG_7_4;WHG_7_5;WHG_7_6;WHG_7_7;WHG_7_8;WHG_7_9;WHG_7_10;WHG_7_11;WHG_8_1;WHG_8_2;WHG_8_3;WHG_8_4;WHG_8_5;WHG_8_6;WHG_8_7;WHG_8_8;WHG_9_1;WHG_9_2;WHG_9_3;WHG_9_4;WHG_9_5"
fields_zensus = [StructField(field_name, StringType(), True) for field_name in schema_string_zensus.split(";")]
schema_zensus = StructType(fields_zensus)


destatis = spark.read.csv(dateipfad_anwendungen + datei_DESTATIS, sep=";", header=True, encoding="ISO-8859-1", schema=schema_destatis)
destatis.printSchema()
destatis.show()
destatis.createOrReplaceTempView("destatis")



destatis_gv = spark.read.csv(dateipfad + datei_DESTATIS_GV, sep=";", header=True, encoding="ISO-8859-1", schema=schema_destatis_gv)
destatis_gv.printSchema()
destatis_gv.show()
destatis_gv.createOrReplaceTempView("destatis_gv")



schema_string_zensus = "AGS_12;RS_Land;RS_RB_NUTS2;RS_Kreis;RS_VB;RS_Gem;Name;Reg_Hier;HH_1_1;HH_1_2;HH_1_3;HH_1_4;HH_1_5;HH_1_6;HH_2_1;HH_2_2;HH_2_3;HH_2_4;HH_2_5;HH_2_6;HH_2_7;HH_2_8;HH_3_1;HH_3_2;HH_3_3;HH_3_4;HH_3_5;HH_3_6;HH_3_7;HH_4_1;HH_4_2;HH_4_3;HH_4_4;FAM_1_1;FAM_1_2;FAM_1_3;FAM_1_4;FAM_2_1;FAM_2_2;FAM_2_3;FAM_2_4;FAM_2_5;FAM_2_6;FAM_3_1;FAM_3_2;FAM_3_3;FAM_3_4;FAM_3_5;FAM_3_6;AEWZ;DEM_1_1;DEM_1_2;DEM_1_3;DEM_2_1;DEM_2_2;DEM_2_3;DEM_2_4;DEM_2_5;DEM_2_6;DEM_2_7;DEM_2_8;DEM_2_9;DEM_2_10;DEM_2_11;DEM_2_12;DEM_2_13;DEM_2_14;DEM_2_15;DEM_2_16;DEM_2_17;DEM_2_18;DEM_2_19;DEM_2_20;DEM_2_21;DEM_2_22;DEM_2_23;DEM_2_24;DEM_2_25;DEM_2_26;DEM_2_27;DEM_3_1;DEM_3_2;DEM_3_3;DEM_3_4;DEM_3_5;DEM_3_6;DEM_3_7;DEM_3_8;DEM_3_9;DEM_3_10;DEM_3_11;DEM_3_12;DEM_3_13;DEM_3_14;DEM_3_15;DEM_3_16;DEM_3_17;DEM_3_18;DEM_3_19;DEM_3_20;DEM_3_21;DEM_3_22;DEM_3_23;DEM_3_24;DEM_3_25;DEM_3_26;DEM_3_27;DEM_3_28;DEM_3_29;DEM_3_30;DEM_4_1;DEM_4_2;DEM_4_3;DEM_4_4;DEM_4_5;DEM_4_6;DEM_4_7;DEM_4_8;DEM_4_9;DEM_4_10;DEM_4_11;DEM_4_12;DEM_4_13;DEM_4_14;DEM_4_15;DEM_4_16;DEM_4_17;DEM_4_18;DEM_4_19;DEM_4_20;DEM_4_21;DEM_4_22;DEM_4_23;DEM_4_24;DEM_4_25;DEM_4_26;DEM_4_27;DEM_4_28;DEM_4_29;DEM_4_30;DEM_4_31;DEM_4_32;DEM_4_33;DEM_4_34;DEM_4_35;DEM_4_36;DEM_5_1;DEM_5_2;DEM_5_3;DEM_5_4;DEM_5_5;DEM_5_6;DEM_5_7;DEM_6_1;DEM_6_2;DEM_6_3;DEM_6_4;DEM_6_5;DEM_6_6;DEM_6_7;REL_1_1;REL_1_2;REL_1_3;REL_1_4;MIG_1_1;MIG_1_2;MIG_1_3;MIG_1_4;MIG_1_5;MIG_1_6;MIG_1_7;MIG_1_8;MIG_1_9;MIG_1_10;MIG_1_11;MIG_2_1;MIG_2_2;MIG_2_3;MIG_2_4;MIG_2_5;MIG_2_6;MIG_2_7;MIG_2_8;MIG_3_1;MIG_3_2;MIG_3_3;MIG_3_4;MIG_3_5;ERW_1_1;ERW_1_2;ERW_1_3;ERW_1_4;ERW_1_5;ERW_1_6;ERW_1_7;ERW_1_8;ERW_1_9;ERW_1_10;ERW_1_11;ERW_1_12;ERW_1_13;ERW_1_14;ERW_1_15;ERW_2_1;ERW_2_2;ERW_2_3;ERW_2_4;ERW_2_5;ERW_2_6;ERW_3_1;ERW_3_2;ERW_3_3;ERW_3_4;ERW_3_5;ERW_3_6;ERW_3_7;ERW_3_8;ERW_3_9;ERW_3_10;ERW_3_11;ERW_4_1;ERW_4_2;ERW_4_3;ERW_4_4;ERW_4_5;ERW_4_6;ERW_4_7;ERW_4_8;ERW_4_9;ERW_4_10;ERW_4_11;ERW_4_12;ERW_4_13;ERW_4_14;ERW_4_15;BIL_2_1;BIL_2_2;BIL_2_3;BIL_2_4;BIL_3_1;BIL_3_2;BIL_3_3;BIL_3_4;BIL_3_5;BIL_3_6;BIL_3_7;BIL_4_1;BIL_4_2;BIL_4_3;BIL_4_4;BIL_4_5;BIL_4_6;BIL_4_7;BIL_4_8;BIL_4_9;BIL_4_10;BIL_5_1;BIL_5_2;BIL_5_3;BIL_5_4;BIL_5_5;BIL_5_6;BIL_5_7;BIL_5_8;GEB_1_1;GEB_1_2;GEB_1_3;GEB_1_4;GEB_1_5;GEB_2_1;GEB_2_2;GEB_2_3;GEB_2_4;GEB_2_5;GEB_2_6;GEB_2_7;GEB_2_8;GEB_2_9;GEB_2_10;GEB_3_1;GEB_3_2;GEB_3_3;GEB_3_4;GEB_3_5;GEB_3_6;GEB_3_7;GEB_3_8;GEB_3_9;GEB_3_10;GEB_3_11;GEB_4_1;GEB_4_2;GEB_4_3;GEB_4_4;GEB_4_5;GEB_4_6;GEB_4_7;GEB_4_8;GEB_4_9;GEB_5_1;GEB_5_2;GEB_5_3;GEB_5_4;GEB_5_5;GEB_5_6;GEB_5_7;GEB_6_1;GEB_6_2;GEB_6_3;GEB_6_4;GEB_6_5;GEB_6_6;GEB_7_1;GEB_7_2;GEB_7_3;GEB_7_4;GEB_7_5;WHG_1_1;WHG_1_2;WHG_1_3;WHG_1_4;WHG_1_5;WHG_2_1;WHG_2_2;WHG_2_3;WHG_2_4;WHG_2_5;WHG_2_6;WHG_2_7;WHG_2_8;WHG_2_9;WHG_2_10;WHG_3_1;WHG_3_2;WHG_3_3;WHG_3_4;WHG_3_5;WHG_3_6;WHG_3_7;WHG_3_8;WHG_3_9;WHG_3_10;WHG_3_11;WHG_4_1;WHG_4_2;WHG_4_3;WHG_4_4;WHG_4_5;WHG_4_6;WHG_4_7;WHG_4_8;WHG_4_9;WHG_5_1;WHG_5_2;WHG_5_3;WHG_5_4;WHG_5_5;WHG_5_6;WHG_5_7;WHG_6_1;WHG_6_2;WHG_6_3;WHG_6_4;WHG_6_5;WHG_7_1;WHG_7_2;WHG_7_3;WHG_7_4;WHG_7_5;WHG_7_6;WHG_7_7;WHG_7_8;WHG_7_9;WHG_7_10;WHG_7_11;WHG_8_1;WHG_8_2;WHG_8_3;WHG_8_4;WHG_8_5;WHG_8_6;WHG_8_7;WHG_8_8;WHG_9_1;WHG_9_2;WHG_9_3;WHG_9_4;WHG_9_5"
fields_zensus = [StructField(field_name, StringType(), True) for field_name in schema_string_zensus.split(";")]
schema_zensus = StructType(fields_zensus)
zensus = spark.read.csv(dateipfad + datei_ZENSUS, sep=";", header=True, encoding="ISO-8859-1", schema=schema_zensus)
zensus.printSchema()
zensus.show()
zensus.createOrReplaceTempView("zensus")


### ANREICHERN: DATENBESTÄNDE VERKNÜPFEN ###

zensus.count() #12544
destatis_gv.count() #11516
destatis.count() #526

join1 = spark.sql("""
                    SELECT 
                    zensus.*,
                    destatis.*
                    FROM zensus
                    LEFT JOIN destatis
                        ON zensus.AGS_12 = destatis.RS
                    """)


#### Von den 12544 Datensätzen besitzen 566 Regionen Merkmale aus dem Destatis Datensatz
## Grund: Da im Zensusdatensatz einige Regionen mehrfach geführt werden (z.B. Hamburg als Gemeinde und Kreisfreie Stadt), exisitieren mehrere Verknüpfungen
## Daher gibt es mehr Verknüpfungen als Datensätze im Destatis Bestand vorhanden sind (526)
join1.count() # 12544 

######################### BEHANDLUNG FEHLENDER WERTE #######################


"""
1.–2. Stelle   = Kennzahl des Bundeslandes 
3. Stelle      = Kennzahl des Regierungsbezirks; wenn nicht vorhanden: 0 
4.–5. Stelle   = Kennzahl des Landkreises oder der kreisfreien Stadt
6.–9. Stelle   = Verbandsschlüssel
10.–12. Stelle = Gemeindekennzahl 

https://www.geodaten-deutschland.de/
https://www.destatis.de/DE/ZahlenFakten/LaenderRegionen/Regionales/Gemeindeverzeichnis/Archiv/ArchivReiter.html
https://de.wikipedia.org/wiki/Datei:Administrative_Gliederung_Deutschlands.svg
"""

daten = join1.toPandas()



def finde_naechsthoeheren_RS(RS, look_up_tabelle):
   """ Suche und RÜckgabe nach einer nächsthöheren Region in einem Look up Datensatz """
    if(RS in look_up_tabelle):
        naechsthoeherer_RS = RS
    else:
        naechsthoeherer_RS = finde_naechsthoeheren_RS(erzeuge_naechsthoeheren_RS(RS), look_up_tabelle)
    
    return naechsthoeherer_RS

def erzeuge_naechsthoeheren_RS(RS):
    """ Erzeugt den Regionschlüssel der nächsthöheren Hierachieebene """
    
    ## String des RS in seine Bestandteile aufteilen
    RS_Land, RS_Reg, RS_Kreis, RS_VG, RS_Gem = RS[:2], RS[2:3], RS[3:5], RS[5:9], RS[9:]
    
    if(RS_Gem > "000"):
        RS_Gem = "000"
    
    elif(RS_VG > "0000"):
        RS_VG = "0000"
    
    elif(RS_Kreis > "00"):
        RS_Kreis = "00"
    
    elif(RS_Reg > "0"):
        RS_Reg = "0"
    
    elif(RS_Land > "00"):
        RS_Land = "00"
    
    else:
        print("Die höchste Hierachieebene ist erreicht")
        
    return RS_Land + RS_Reg + RS_Kreis + RS_VG + RS_Gem
    

finde_naechsthoeheren_RS("010510000000", look_up_tabelle)


erzeuge_naechsthoeheren_RS("010510011011")
erzeuge_naechsthoeheren_RS("010510011000")
erzeuge_naechsthoeheren_RS("010510000000")
erzeuge_naechsthoeheren_RS("010000000000")
erzeuge_naechsthoeheren_RS("000000000000")

spalten_mit_fehlenden_werten = [
  "EINK_JE_EINW", "ANTEIL_ARBEITSLOSE", "ANTEIL_ARBEITSLOSE_ALTER_15_24", "ANTEIL_ARBEITSLOSE_ALTER_55_64", 
  "ANTEIL_LANGZEITARBEITSLOSE", "ANTEIL_ARBEITSLOSE_AUSLAENDER", "ANTEIL_SIEDLUNG_VERKEHR", 
  "ANTEIL_ERHOLUNGSFLAECHE", "ANTEIL_LANDWIRTSCHAFTSFLAECHE", "ANTEIL_WALDFLAECHE"]



daten_mit_fehlenden_werten = daten[daten[spalten_mit_fehlenden_werten].isnull().any(axis=1)]
daten_ohne_fehlenden_werten = daten[~daten[spalten_mit_fehlenden_werten].isnull().any(axis=1)]

daten_mit_fehlenden_werten.count() 
daten_ohne_fehlenden_werten.count() 


len(daten_mit_fehlenden_werten) #11978
len(daten_ohne_fehlenden_werten) #566


daten_mit_fehlenden_werten.dtypes



### AGS_12 = RS
look_up_tabelle = daten_ohne_fehlenden_werten["AGS_12"].unique()
##über RS iterieren die null-Werte einhalten
for RS in daten_mit_fehlenden_werten["AGS_12"].unique():
    
    #Für jedes Objekt, fülle spaltenweise die missing-values
    for spalte in spalten_mit_fehlenden_werten:
       #print(RS, col)
       #Falls eine Spalte missing values enthält, dann schau im look-up-table nach Referenzwerten
       if(daten_mit_fehlenden_werten.loc[daten_mit_fehlenden_werten["AGS_12"] == RS, spalte].isnull().iloc[0]):
           #Befülle die Spalte im Dataframe mit den Werten aus dem hierchisch höheren Gebiet 
           vorhandene_werte_hoherer_hierachie = [wert for wert in daten_ohne_fehlenden_werten.loc[daten_ohne_fehlenden_werten["AGS_12"] == finde_naechsthoeheren_RS(RS, look_up_tabelle), spalte]]
           daten_mit_fehlenden_werten.loc[daten_mit_fehlenden_werten["AGS_12"] == RS, spalte] = vorhandene_werte_hoherer_hierachie 


# test 
daten_mit_fehlenden_werten.count() # Merkmale sind nun voll besetzt


daten_mit_fehlenden_werten.dtypes
join1_gefuellt = daten_mit_fehlenden_werten.append(daten_ohne_fehlenden_werten)

join1_gefuellt.dtypes
len(join1_gefuellt) # 12544




### ANREICHERN: DATENBESTÄNDE VERKNÜPFEN ######

join2 = join1_gefuellt.set_index("AGS_12").join(destatis_gv.toPandas().set_index("RS"), how="inner")

## Durch die Reformierung und Umstrukutierung der Verwaltungsgliederung im Jahre 2011 gibt für einige Regionen unterschiedliche Schlüssel
## Dies betrifft mehrheitlich Regionen aus Sachsen und Sachsen-Anhalt.
len(join2) #11400





#### BEHANDLUNG VON FEHLENDEN WERTEN ########



# 0% Missing Value Toleranz 
len(join2.dropna(axis="columns" , thresh=len(join2)).dtypes) #52

# 10% Missing Value Toleranz 

schwellwert = len(join2) - (len(join2)/100 * 10) # Datensatzlänge - 10%
len(join2.dropna(axis="columns" , thresh=schwellwert).dtypes) #242

zensus.dropna(axis="columns" , thresh=10260) # 242 Spalten verbleibend


# 20% Missing Value Toleranz 

schwellwert = len(join2) - (len(join2)/100 * 20) # Datensatzlänge - 20%
len(join2.dropna(axis="columns" , thresh=schwellwert).dtypes) #258


# 30% Missing Value Toleranz 

schwellwert = len(join2) - (len(join2)/100 * 30) # Datensatzlänge - 30%
len(join2.dropna(axis="columns" , thresh=schwellwert).dtypes) #266



werte = []
for anzahl_nullwerte in range(len(join2), 0, -1):
    
    print(anzahl_nullwerte)
    schwellwert = len(join2) - anzahl_nullwerte
    werte.append([anzahl_nullwerte, len(join2.dropna(axis="columns" , thresh=schwellwert).dtypes)])

anzahl_fehlender_werte = [x[0] for x in werte]
anzahl_vorhandener_spalten = [x[1] for x in werte]


import matplotlib.pyplot as plt
plt.plot(anzahl_fehlender_werte.sort_index(ascending = False), anzahl_vorhandener_spalten)
plt.ylabel("Anzahl vorhandener Spalten im Datensatz")
plt.xlabel("Anzahl erforderlicher Werte je Spalte")
plt.show()




dateipfad_neu = pfad +  "\\2_curated\\anwendungen\\filialnetzoptimierung\\daten\\"
datei_neu = "DESTATIS_ZENSUS_FEHLENDE_WERTE_BEHANDELT.csv"

len(join2)
len(join2.dropna(axis="columns" , thresh=10500).dtypes) #233


join2_reduziert = join2.dropna(axis="columns" , thresh=10500)

join2_reduziert.to_csv(dateipfad + datei_neu, sep=";", header=True, index=False, encoding="ISO-8859-1")
