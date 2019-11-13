# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
pd.options.display.float_format = '{:.2f}'.format


pfad = os.path.abspath(".")
dateipfad_COMP = pfad + "\\data\\company\\"
datei_COMP = "COMP_GEO_ENRICH_REGION.csv"

dateipfad_DATA = pfad + "\\data\\share\\"
datei_DATA = "data.csv"


df_comp = pd.read_csv(dateipfad_COMP + datei_COMP, sep=";", dtype=object, encoding="ISO-8859-1")
df_comp.head()
df_comp.dtypes

df_full_data = pd.read_csv(dateipfad_DATA + datei_DATA, sep=";", dtype=object, encoding="ISO-8859-1")
df_full_data.head()
df_full_data.columns

df_full_data[df_full_data["AGS_8"].str.contains("09371000")]
df_full_data[df_full_data["Name"].str.contains("Bayreuth")]

test = df_full_data[df_full_data.isnull()].count()
df_full_data.isnull().sum()
test[test > 0 ]

df_full_data[df_full_data.sort_values(by=["AGS_12"]).duplicated(subset=["AGS_12_filled", "Name"], keep="last")][["AGS_12", "Name"]]


df_full_data = df_full_data.sort_values(by=["AGS_12"]).drop_duplicates(subset=["AGS_12_filled", "Name"], keep="last")

    # Umsatzschätzung
    #Beispiel: https://de.statista.com/statistik/daten/studie/365021/umfrage/jahresumsatz-pro-filiale-der-drogeriemaerkte-rossmann-in-deutschland/

    """
    # Infrastruktur-Merkmale
    region["osm_nodes_counter"], region["ANTEIL_SIEDLUNG_VERKEHR"], region["BEV_JE_QM"], region["BESIEDLUNGSSCHLUESSEL"]
    region["GEB_6_4"] # Gebäude mit Anzahl Wohnungen 3-6
    
     # Bevölkerungs-Merkmale
    region["HH_1_4"] # Haushalte mit Paare und Kindern , region["DEM_2_7"] # Anzahl verheiratete Personen ingesamt, region["EINK_JE_EINW"] region["ANTEIL_ARBEITSLOSE"]
    """



def estimate_revenue(region):

    infrastruktur_value = (int(region["GEB_6_4"]) + int(region["osm_nodes_counter"])*4 + float(region["ANTEIL_SIEDLUNG_VERKEHR"])*2 + float(region["BEV_JE_QM"]))*5
    
    bev_value = int(region["HH_1_4"])*5 + int(region["DEM_2_7"])*2 + float(region["EINK_JE_EINW"])*10 - (float(region["ANTEIL_ARBEITSLOSE"])* int(region["BEV_INSGESAMT"]))/2
    
    value = infrastruktur_value * bev_value * np.random.randint(9, 15)
    revenue = (value / np.log2(value)**5 ) * np.random.randint(900000, 1500000)
    
    return round(revenue, 2)

estimate_revenue(df_full_data[df_full_data["AGS_12_filled"] == df_comp["region_rs_for_estimation"][10]])
estimate_revenue(df_full_data[df_full_data["AGS_12_filled"] == df_comp["region_rs_for_estimation"][11]])

def estimate_revenue_log(region):
   
    infrastruktur_value = int(region["GEB_6_4"]) + int(region["osm_nodes_counter"])**4 + float(region["ANTEIL_SIEDLUNG_VERKEHR"])**2 + float(region["BEV_JE_QM"])**5
    
    bev_value = int(region["HH_1_4"])**3 + int(region["DEM_2_7"]) + float(region["EINK_JE_EINW"])**7 - (float(region["ANTEIL_ARBEITSLOSE"])* int(region["BEV_INSGESAMT"]))**2
    
    value = (infrastruktur_value * bev_value * np.random.randint(50, 80))/ np.random.randint(80, 100)
    revenue = (np.log2(value)/int(region["BESIEDLUNGSSCHLUESSEL"])) * 30000000
    
    return round(revenue, 2)
    
estimate_revenue_log(df_full_data[df_full_data["AGS_12_filled"] == df_comp["region_rs_for_estimation"][10]])
estimate_revenue_log(df_full_data[df_full_data["AGS_12_filled"] == df_comp["region_rs_for_estimation"][11]])

def estimate_sales_area(revenue):
    
    sales_area = ((float(revenue) / np.random.randint(5000, 7000))**0.5 ) + np.random.randint(50, 120)
    
    return int(sales_area)

def estimate_numb_of_empl(revenue):
    
    numb_of_empl = (float(revenue) / np.random.randint(50000, 70000))**0.4 + np.random.randint(12, 21)
    
    return int(numb_of_empl)



unmatched_branch = []
new_branches = []

for branch in df_comp.itertuples():
      
    branch_enrich = {}

    branch_region = df_full_data[df_full_data["AGS_12"] == branch.region_rs_for_estimation]
    
    if(len(branch_region) == 0):
        unmatched_branch.append(branch)
        continue
   
    branch_revenue = estimate_revenue(branch_region)
    branch_revenue_log = estimate_revenue_log(branch_region)
    branch_sales_area, branch_numb_of_empl = estimate_sales_area(branch_revenue), estimate_numb_of_empl(branch_revenue)
    
    new_branch = {"ID": branch.branch_id,
                  "Strassenname": branch.Strassenname,
                  "PLZ": branch.PLZ,
                  "Region": branch.Region,
                  "revenue1": branch_revenue,
                  "revenue2": branch_revenue_log,
                  "sales_area": branch_sales_area,
                  "number_of_employees": branch_numb_of_empl,
                  "region_counter": branch.region_counter,
                  "region_list": branch.region_list,
                  "gemeinde_rs": branch.gemeinde_rs,
                  "kreis_stadt_rs": branch.kreis_stadt_rs,
                  "krfr_stadt_rs": branch.krfr_stadt_rs,
                  "region_rs_for_estimation": branch.region_rs_for_estimation
                  }
    
    new_branches.append(new_branch)


len(new_branches) #3260

len(unmatched_branch) #88
df_unmatched = pd.DataFrame(unmatched_branch)
len(df_unmatched["region_rs_for_estimation"].unique()) #24

df_comp_enrich_attributes = pd.DataFrame(new_branches)
df_comp_enrich_attributes.dtypes
df_comp_enrich_attributes.head(2)
df_comp_enrich_attributes.describe()



df_comp_enrich_attributes.to_csv(dateipfad_COMP + "COMP_GEO_REGION_ENRICH_ATTRIBUTES.csv", sep=";", header=True, index=False, encoding="ISO-8859-1")


df_comp_enrich_attributes[["revenue", "revenue_log", "sales_area", "number_of_employees"]].describe()


# max: 12.453.152.470
# mean: 6.510.073.644
# min: 1.305.244.385


df_comp_enrich_attributes.boxplot(column=["revenue"])


df_comp_enrich_attributes["log_revenue"] = df_comp_enrich_attributes["revenue"].apply(lambda x: np.log2(x))
df_comp_enrich_attributes.boxplot(column=["log_revenue"])