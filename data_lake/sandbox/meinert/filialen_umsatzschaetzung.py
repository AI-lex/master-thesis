# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
pd.options.display.float_format = '{:.2f}'.format

## Dateipfade und Dateien
pfad = os.path.abspath(".")
dateipfad_COMP = pfad + "\\data\\company\\"
datei_COMP = "COMP_GEO_ENRICH_REGION.csv"

dateipfad_DATA = pfad + "\\data\\share\\"
datei_DATA = "data.csv"

## Datei mit Filialen in eine Datei laden
filialen = pd.read_csv(dateipfad_COMP + datei_COMP, sep=";", dtype=object, encoding="ISO-8859-1")
filialen.head()
filialen.dtypes

## Datei mit R in eine Datei laden
regionsdaten = pd.read_csv(dateipfad_DATA + datei_DATA, sep=";", dtype=object, encoding="ISO-8859-1")
regionsdaten.head()
regionsdaten.columns

## Dubletten identifizieren
regionsdaten[regionsdaten.sort_values(by=["AGS_12"]).duplicated(subset=["AGS_12_filled", "Name"], keep="last")][["AGS_12", "Name"]]

## Dubletten filtern
regionsdaten = regionsdaten.sort_values(by=["AGS_12"]).drop_duplicates(subset=["AGS_12_filled", "Name"], keep="last")

    # Umsatzschätzung
    #Beispiel: https://de.statista.com/statistik/daten/studie/365021/umfrage/jahresumsatz-pro-filiale-der-drogeriemaerkte-rossmann-in-deutschland/

    """
    # Infrastruktur-Merkmale
    region["osm_nodes_counter"], region["ANTEIL_SIEDLUNG_VERKEHR"], region["BEV_JE_QM"], region["BESIEDLUNGSSCHLUESSEL"]
    region["GEB_6_4"] # Gebäude mit Anzahl Wohnungen 3-6
    
     # Bevölkerungs-Merkmale
    region["HH_1_4"] # Haushalte mit Paare und Kindern , region["DEM_2_7"] # Anzahl verheiratete Personen ingesamt, region["EINK_JE_EINW"] region["ANTEIL_ARBEITSLOSE"]
    """

def schaetze_umsatz(region):
    """Schätzt den Filialumsatz anhand Bevölkerungs- und Infrastrukturmerkmale des Filalstandorts"""
    
    infrastruktur = (int(region["GEB_6_4"]) + int(region["osm_nodes_counter"])*4 + float(region["ANTEIL_SIEDLUNG_VERKEHR"])*2 + float(region["BEV_JE_QM"]))*5
    
    bevoelkerung = int(region["HH_1_4"])*5 + int(region["DEM_2_7"])*2 + float(region["EINK_JE_EINW"])*10 - (float(region["ANTEIL_ARBEITSLOSE"])* int(region["BEV_INSGESAMT"]))/2
    
    wert = infrastruktur * bevoelkerung * np.random.randint(9, 15)
    
    ## Umsatz auf einen Wertebereich von 400.000 bis 3 Milliarden skalieren
    umsatz = (wert / np.log2(value)**5 ) * np.random.randint(900000, 1500000)
    
    return round(revenue, 2)


def schaetze_verkaufsflaeche(umsatz):
    """ Schätzt die Verkaufsfläche einer Region anhand des Filalumsatzes"""
    ## Verkaufsfläche auf 200 bis 1000 skalieren
    verkaufsflaeche = ((float(umsatz) / np.random.randint(5000, 7000))**0.5 ) + np.random.randint(50, 120)
    
    return int(verkaufsflaeche)

def schaetze_anzahl_mitarbeiter(umsatz):
    """ Schätzt die Mitarbeiteranzahl einer Region anhand des Filalumsatzes"""

    ## Anzahl Mitarbeiter auf 15 bis 300 skalieren
    anzahl_mitarbeiter = (float(umsatz) / np.random.randint(50000, 70000))**0.4 + np.random.randint(12, 21)
    
    return int(anzahl_mitarbeiter)



unmatched_branch = []
new_branches = []
## Über alle FIlalen interieren und die Region ausfindig machen, in der sich eine Filiale befindet
for filiale in df_comp.itertuples():
      
    filialen_angereichert = {}

    filialstandort = regionsdaten[regionsdaten["AGS_12"] == filiale.region_fuer_schaetzung]
    
    if(len(filialstandort) == 0):
        unmatched_filiale.append(filiale)
        continue
   
    filiale_umsatz = schaetze_umsatz(branch_region)
    verkaufsflaeche, anzahl_mitarbeiter = schaetze_verkaufsflaeche(filiale_umsatz), schaetze_anzahl_mitarbeiter(filiale_umsatz)
    
    filiale_neu = {"ID": filiale.branch_id,
                  "Strassenname": filiale.Strassenname,
                  "PLZ": filiale.PLZ,
                  "Region": filiale.Region,
                  "revenue": filiale,
                  "sales_area": verkaufsflaeche,
                  "number_of_employees": anzahl_mitarbeiter,
                  "region_counter": filiale.region_counter,
                  "region_list": filiale.region_list,
                  "gemeinde_rs": filiale.gemeinde_rs,
                  "kreis_stadt_rs": filiale.kreis_stadt_rs,
                  "krfr_stadt_rs": filiale.krfr_stadt_rs,
                  "region_rs_for_estimation": filiale.region_fuer_schaetzung
                  }
    
    filialen_angereichert.append(filiale_neu)


len(filialen_angereichert) #3260

len(unmatched_filiale) #88
df_unmatched = pd.DataFrame(unmatched_branch)
len(df_unmatched["region_rs_for_estimation"].unique()) #24

filialen_angereichert = pd.DataFrame(filialen_angereichert)
filialen_angereichert.dtypes
filialen_angereichert.head(2)
filialen_angereichert.describe()



filialen_angereichert.to_csv(dateipfad_COMP + "COMP_GEO_REGION_ENRICH_ATTRIBUTES.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")


filialen_angereichert[["revenue", "revenue_log", "sales_area", "number_of_employees"]].describe()


# max: 12.453.152.470
# mean: 6.510.073.644
# min: 1.305.244.385


filialen_angereichert.boxplot(column=["revenue"])


filialen_angereichert["log_revenue"] = filialen_angereichert["revenue"].apply(lambda x: np.log2(x))
filialen_angereichert.boxplot(column=["log_revenue"])