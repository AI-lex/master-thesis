# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np


pfad = os.path.abspath(".")
dateipfad = pfad +  "\\2_curated\\daten\\"
datei_DESTATIS = "DESTATIS_INSOLVENZ_ARBEITSLOSIGKEIT_EINKOMMEN_GEBIETSART.csv"



### Datentypen von ganzzahligen Merkmalen müssen hier als float definiert werden, weil dies sonst zu einem
### Fehler führt, da die fehlenden Werte nicht dargestellt werden können. 

schema = {"JAHR": "int", "RS": "str", "GEBIET": "str", "EINK_JE_EINW": "float", "ANTEIL_ARBEITSLOSE": "float",\
          "ANTEIL_ARBEITSLOSE_ALTER_15_24": "float", "ANTEIL_ARBEITSLOSE_ALTER_55_64": "float", "ANTEIL_LANGZEITARBEITSLOSE": "float",\
          "ANTEIL_ARBEITSLOSE_AUSLAENDER": "float", "ANZAHL_BEANTR_VERFAHREN": "float",
          "ANZAHL_EROEFFNETE_VERFAHREN": "float", "ANZAHL_ABGEW_VERFAHREN": "float",\
          "ANZAHL_BEANTR_VERFAHREN_MIT_SCHULDPLAN": "float", "ANZAHL_BESCHAEFTIGTE": "float",\
          "FORDERUNGEN_TSD_EURO": "float", "ANTEIL_SIEDLUNG_VERKEHR": "float",\
          "ANTEIL_ERHOLUNGSFLAECHE": "float", "ANTEIL_LANDWIRTSCHAFTSFLAECHE": "float",\
          "ANTEIL_WALDFLAECHE": "float"} 

daten = pd.read_csv(dateipfad + datei_DESTATIS, sep=";", dtype=schema, encoding="ISO-8859-1")
daten.dtypes


spalten_mit_fehlenden_werten = daten.columns[3:19] 


### Auf Nullwerte überprüfen
### geht nicht 
daten["EINK_JE_EINW"][24] == np.nan
## geht 
daten["EINK_JE_EINW"].isnull()[24]



## Damit über die Interpolation die Zeitreihe in der richtigen Folge ist (von Jahr 2010 bis 2015),
## müssen die Daten zunächst nach Jahr sortiert werden
daten_sortiert = daten.sort_values(by=["RS", "JAHR"], ascending=[True, True]).reset_index(drop=True)
daten_sortiert[["JAHR", "RS"]].head(20)



def schaetze_werte(zeitreihe):
    """ Schätzt die Werte einer Zeitreihe über pchip und lineare Verfahren """

    try: #pchip besseres monotonie-verhalten als spline
        zeitreihe = zeitreihe.interpolate(method="spline", order=2, limit_direction="both")
    except: #Falls zu wenig Inputvariablen sind, dann linear schätzen
        zeitreihe = zeitreihe.interpolate(method="linear", limit_direction="both")

    return zeitreihe



## Die Zeitreihe kann nach der Sortierung mit einer Gruppierung nach Region erfolgen. Auf den Gruppen kann 
## kann das die Schätzfunktion angewendet werden
    
## Vorher
daten_sortiert.count()

## Für jede Region werden die Zeitreihen aggriert (grouby) und dann geschätzt
daten_fehlende_werte_geschaetzt =  daten_sortiert.groupby("RS").apply(lambda zeitreihe: schaetze_werte(zeitreihe))

## Nachher
daten_fehlende_werte_geschaetzt.count()


dateipfad_neu = pfad +  "\\2_curated\\anwendungen\\filialnetzoptimierung\\daten\\"
datei_neu = "DESTATIS_INSOLVENZ_ARBEITSLOSIGKEIT_EINKOMMEN_GEBIETSART_ZEITREIHE_GESCHAETZT.csv"

daten_fehlende_werte_geschaetzt.to_csv(dateipfad_neu + datei_neu, sep=";", header=True, index=False, encoding="ISO-8859-1")

daten_2011 = daten_fehlende_werte_geschaetzt[daten_fehlende_werte_geschaetzt["JAHR"] == 2011].copy()



############ Hierachische Schätzung  ##############


def finde_naechsthoeheren_RS(RS, look_up_tabelle):
   
    if(RS in look_up_tabelle):
        naechsthoeherer_RS = RS
    else:
        naechsthoeherer_RS = finde_naechsthoeheren_RS(erzeuge_naechsthoeheren_RS(RS), look_up_tabelle)
    
    return naechsthoeherer_RS

def erzeuge_naechsthoeheren_RS(RS):
    
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
    

finde_naechsthoeheren_RS("031000000000", look_up_tabelle)


erzeuge_naechsthoeheren_RS("010515169052")
erzeuge_naechsthoeheren_RS("010515169000")
erzeuge_naechsthoeheren_RS("010510000000")
erzeuge_naechsthoeheren_RS("010000000000")
erzeuge_naechsthoeheren_RS("000000000000")


spalten_mit_fehlenden_werten = [
  "EINK_JE_EINW", "ANTEIL_ARBEITSLOSE", "ANTEIL_ARBEITSLOSE_ALTER_15_24", "ANTEIL_ARBEITSLOSE_ALTER_55_64", 
  "ANTEIL_LANGZEITARBEITSLOSE", "ANTEIL_ARBEITSLOSE_AUSLAENDER", "ANTEIL_SIEDLUNG_VERKEHR", 
  "ANTEIL_ERHOLUNGSFLAECHE", "ANTEIL_LANDWIRTSCHAFTSFLAECHE", "ANTEIL_WALDFLAECHE"]



daten_mit_fehlenden_werten = daten_2011[daten_2011[spalten_mit_fehlenden_werten].isnull().any(axis=1)]
daten_ohne_fehlenden_werten = daten_2011[~daten_2011[spalten_mit_fehlenden_werten].isnull().any(axis=1)]

daten_mit_fehlenden_werten.count() 
daten_ohne_fehlenden_werten.count() 
daten_mit_fehlenden_werten["RS"]

len(daten_mit_fehlenden_werten) #111
len(daten_ohne_fehlenden_werten) #415




look_up_tabelle = daten_ohne_fehlenden_werten["RS"].unique()
##über RS iterieren die null-Werte einhalten
for RS in daten_mit_fehlenden_werten["RS"].unique():
    
    #Für jedes Objekt, fülle spaltenweise die missing-values
    for spalte in spalten_mit_fehlenden_werten:
       #print(RS, col)
       #Falls eine Spalte missing values enthält, dann schau im look-up-table nach Referenzwerten
       if(daten_mit_fehlenden_werten.loc[daten_mit_fehlenden_werten["RS"] == RS, spalte].isnull().iloc[0]):
           #Befülle die Spalte im Dataframe mit den Werten aus dem hierachisch höheren Region 
           vorhandene_werte_hoeherer_hierachie = [wert for wert in daten_ohne_fehlenden_werten.loc[daten_ohne_fehlenden_werten["RS"] == finde_naechsthoeheren_RS(RS, look_up_tabelle), spalte]]
           
           daten_mit_fehlenden_werten.loc[daten_mit_fehlenden_werten["RS"] == RS, spalte] = vorhandene_werte_hoeherer_hierachie



daten_werte_geschaetzt_2 = daten_ohne_fehlenden_werten.append(daten_mit_fehlenden_werten, ignore_index=False)

## Test 
daten_werte_geschaetzt_2.count()
daten_werte_geschaetzt_2 = daten_werte_geschaetzt_2.drop("JAHR", axis=1)

datei_neu = "DESTATIS_INSOLVENZ_ARBEITSLOSIGKEIT_EINKOMMEN_GEBIETSART_HIERACHIE_GESCHAETZT_2011.csv"

daten_werte_geschaetzt_2.to_csv(dateipfad_neu + datei_neu, sep=";", header=True, index=False, encoding="ISO-8859-1")























daten_mit_fehlenden_werten = daten_fehlende_werte_geschaetzt_1[daten_fehlende_werte_geschaetzt_1.isnull().any(axis=1)]
daten_ohne_fehlenden_werten = daten_fehlende_werte_geschaetzt_1[~daten_fehlende_werte_geschaetzt_1.isnull().any(axis=1)]

daten_ohne_fehlenden_werten.head() #2490 rows

daten_mit_fehlenden_werten.head() # 666 rows -> 26,75%

len(daten_mit_fehlenden_werten["RS"].unique()) # 111 Objekte für 6 Zeiteinheiten
look_up_tabelle = daten_ohne_fehlenden_werten["RS"].unique() #415 Objekte für 6 Zeiteinheiten





def finde_naechsten_RS(RS, look_up_tabelle):

    if(int(RS)<100):
        

       naechster_RS =  int(str(RS)[:1])
    
    elif(int(RS)<1000):
        naechster_RS = int(str(RS)[:2])
    else:
        index_des_naechsten_RS = (np.abs(look_up_tabelle - RS)).argmin()
        naechster_RS = look_up_tabelle[index_des_naechsten_RS]
        
    return naechster_RS


print(finde_naechsten_RS("15364", look_up_tabelle))


def finde_naechsthoeheren_RS(RS, look_up_tabelle):
    
    
    RS = RS[:-1]
    print(RS)
    if(RS in look_up_tabelle):
        naechsthoeheren_RS = RS
    else:
        naechsthoeheren_RS = finde_naechsthoeheren_RS(RS, look_up_tabelle)
    
    return naechsthoeheren_RS   
        

print(finde_naechsthoeheren_RS("153640000000", look_up_tabelle))



## Über alle Regionen (genauer RS) iterieren, die fehlende Werte besitzen
for RS in daten_mit_fehlenden_werten["RS"].unique():
    #Für jedes Objekt, fülle spaltenweise die missing-values
    for spalte in spalten_mit_fehlenden_werten:
       #print(RS, col)
       #Falls eine Spalte missing values enthält, dann schau im look-up-table nach Referenzwerten
       if(daten_mit_fehlenden_werten.loc[daten_mit_fehlenden_werten["RS"] == RS, spalte].isnull().iloc[0]):
           #Befülle die Spalte im Dataframe mit den Werten aus dem hierchisch höheren Gebiet 
           daten_mit_fehlenden_werten.loc[daten_mit_fehlenden_werten["RS"] == RS, spalte] = [wert for wert in daten_ohne_fehlenden_werten.loc[daten_ohne_fehlenden_werten["RS"] == finde_naechsthoeheren_RS(RS, look_up_tabelle), spalte]]
           print(RS)


#die nun gefüllten werte zusammenführen
daten_werte_geschaetzt_2 = daten_ohne_fehlenden_werten.append(daten_mit_fehlenden_werten, ignore_index=False)



#test ob es noch fehlende werte gibt
daten_werte_geschaetzt_2.count() # 0


##Vorsicht!! INSOLVENZDATEN SIND FALSCH GESCHÄTZT, da diese Angabe kein relativer Faktor. Was ist ein guter Bezug? Anzahl Einwohner?
daten_werte_geschaetzt_2.to_csv(dateipfad + "STAT_FULL_TABLE_FILLED.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")


















test_df = df_stat_sorted_filled_1[df_stat_sorted_filled_1["RS"] == 13072]

df_stat_sorted = df_stat.sort_values(by=["RS", "JAHR"])
df_stat_sorted = df_stat_sorted.reset_index(drop=True)
df_stat_sorted[["JAHR", "RS"]].head(20)




#df_stat_sorted_filled_1 =  df_stat_sorted.groupby(["JAHR", "RS"]).apply(lambda group: group.fillna(method="ffill"))

df_stat_sorted_filled_1 =  df_stat_sorted.groupby("RS").apply(lambda group: interpolate_values(group))

df_stat_sorted_filled_1.to_csv(dateipfad + "STAT_FULL_FILLED_TABLE_1.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")


df_stat_sorted_filled_1 =  df_stat_sorted_filled_1.groupby("RS").apply(lambda group: group.interpolate(method="linear", limit_direction="backwards"))

test_df = df_stat_sorted_filled_1[df_stat_sorted_filled_1["RS"] == 13072]
test_df["ANTEIL_ARBEITSLOSE"] = test_df["ANTEIL_ARBEITSLOSE"].interpolate(method="spline", order=1, limit_direction="both")
test_df = test_df.interpolate(method="spline", order=1, limit_direction="both")

s1 = pd.Series([8, 8.7, 9.3, 9.7, np.nan, np.nan])

s1.interpolate(method="linear", limit_direction="forward", limit=1).interpolate(method="linear", limit_direction="forward", limit=1)
s1.interpolate(method="polynomial", order=1, limit_direction="forward")

s2 = pd.Series([np.nan, np.nan, 9.7, 9.3, 8.7, 8])

s2.interpolate(method="spline", order=1, limit_direction="both")

s3 = pd.Series([9.3 ,np.nan, np.nan, np.nan, np.nan, 9.7])

s3.interpolate(method="spline", order=1, limit_direction="forward")


s4 = pd.Series([0.1 ,np.nan, 0.1, 0, 0, np.nan])


s4.interpolate(method="spline", order=2, limit_direction="forward")


s4.interpolate(method="pchip", order=2, limit_direction="forward")


df_stat_est = df_stat.groupby("RS").apply(lambda group: interpolate_values(group))
df_stat_est[df_stat_est["ANZAHL_EROEFFNETE_VERFAHREN"] < 0]