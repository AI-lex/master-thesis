# -*- coding: utf-8 -*-


import os
import pandas as pd


pfad = os.path.abspath(".")
dateipfad = pfad +  "\\1_raw\\geodaesie\\daten\\"
datei_GEMEINDEGRENZEN = "GEOD_GEMEINDEGRENZEN.geojson"
datei_KREISGRENZEN = "GEOD_KREISGRENZEN.geojson"


df_gemeindegrenzen = pd.read_json(dateipfad + datei_GEMEINDEGRENZEN, encoding="UTF-8")
df_gemeindegrenzen.dtypes
df_gemeindegrenzen.count() # 11649

df_kreisgrenzen = pd.read_json(dateipfad + datei_KREISGRENZEN, encoding="UTF-8")
df_kreisgrenzen.dtypes
df_kreisgrenzen.count() # 439


###### ANREICHERUNG: VERKNÜPFUNG DER DATENSÄTZE. Nachfolgend können diese nach bestimmten Attributen geparsed werden ######


df_gem_kreis = df_gemeindegrenzen["features"].append(df_kreisgrenzen["features"]) ##
df_gem_kreis.count() # 12088


###### BEREINIGUNG: KORREKTE DARSTELLUNG DER REGIONSUMRISSE UND ELEMINIERUNG VON DUBLETTEN BZW.NICHT RELEVANTEN DATEN  ######

# Spaltenbeschreibungen unter 0_landing\geodaesie\meta\meta_VG250.pdf

spalten = ["AGS", "DEBKG_ID", "DES", "GEN", "GF", "LENGTH", "OBJECTID", "OBJECTID_1", "RAU_RS", "RS", "RS_ALT", "Shape_Leng", "Shape__Area", "Shape__Length", "USE_", "WIRKSAMKEI", "valid_geojson", "feature_type"]

# DataFrame erzeugen in dem die geografischen Umrisse gespeichert werden
df_gem_kreis_parsed = pd.DataFrame()

# Über die geografischen Objekte (feature) im Datensatz iterieren und Struktur auslesen:
for feature in df_gem_kreis:
    
    # VORSICHT: feature["geometry"] löst die Struktur des geojson Objekts auf, sodass es nicht mehr valide ist. 
    # Daher zuvor mit str() casten
    strukturiertes_feature = pd.Series(feature["properties"])\
                        .append(pd.Series(str(feature["geometry"])), ignore_index=True)\
                        .append(pd.Series(feature["geometry"]["type"]), ignore_index=True)
    
    # Wird als Zeile im neuen DataFrame hinzugefügt
    df_gem_kreis_parsed = df_gem_kreis_parsed.append(strukturiertes_feature, ignore_index=True)
    
df_gem_kreis_parsed.columns = spalten 
df_gem_kreis_parsed.dtypes
df_gem_kreis_parsed.count() # 12088
df_gem_kreis_parsed.head(1)


##### Unrelevante Daten (werden zunächst beibehalten)
df_gem_kreis_parsed["WIRKSAMKEI"].unique() 
df_gem_kreis_parsed[df_gem_kreis_parsed["WIRKSAMKEI"] == "2011-01-01T00:00:00.000Z"] #81 Gemeindegr.und 16 Kreisegr.  sind erst seit 2011 geltend

relevante_spalten = ["AGS", "RS", "RS_ALT", "RAU_RS","DES", "GEN", "Shape_Leng" , "Shape__Area", "Shape__Length", "valid_geojson", "feature_type"]

df_gem_kreis_extrahiert = df_gem_kreis_parsed[relevante_spalten].copy()

###### Duletten filtern
len(df_gem_kreis_extrahiert["RS"].unique()) #11975 von 12088 sind unique 


## RS ist nicht unique, da ein RS mit mehreren Geofaktoren geführt werden können
# Beispiel: {"type": "Polygon", "coordinates": [[[9.49194956489906, 54.8226356715221], [9.49135232357568, 54.8229080931596], [9.49119503395933, 54.8233119144166], [9.49081877370426, 54.8236074033734], [9.49068579878772, 54.8236511582473], [9.49018611360585, 54.8238155801607], [9.48954094446712, 54.8237228459009], [9.48934085372074, 54.8236404922637], [9.48913201877337, 54.8234603443582], [9.4886897772607, 54.8230788494661], [9.48817802590795, 54.8228615909754], [9.48718230360263, 54.8228110365118], [9.48629733189373, 54.8227613571219], [9.48581538394719, 54.8228427954972], [9.48534107707042, 54.8226045044262], [9.48478631797514, 54.8226427223218], [9.48344968142624, 54.8229518493087], [9.48274418412794, 54.8231167693378], [9.48204277236757, 54.8231111635418], [9.48137571937022, 54.8232124269104], [9.48078198327308, 54.8233355919505], [9.480007244731, 54.8233080692483], [9.47934428355976, 54.8232387992304], [9.4784634162534, 54.8230185415194], [9.47724928475642, 54.8228382412746], [9.47686150450614, 54.8229509244936], [9.47546444637291, 54.8233568780168], [9.475243976446, 54.8233124645396], [9.47549943305826, 54.8225336800962], [9.47552296712209, 54.8224619344383], [9.47420223704193, 54.822110192508], [9.47320964177211, 54.8219316369651], [9.47176891024469, 54.8219626493193], [9.47077116044179, 54.8219972279911], [9.46902578325208, 54.8224094961798], [9.468537592014311, 54.8227466541968], [9.46882826078909, 54.8229408819415], [9.46758771882277, 54.8235151105947], [9.46733450171048, 54.8236323170777], [9.46713717867348, 54.8235337044461], [9.46503628422692, 54.822483730732], [9.46402285972063, 54.8219256030014], [9.46303246891651, 54.8213801375541], [9.46150968077949, 54.8202377930865], [9.46046057788502, 54.8193551327126], [9.45982898049361, 54.8190519344344], [9.45951180962128, 54.8188996727211], [9.45799600498727, 54.8174801852893], [9.45741202549243, 54.8166385655949], [9.45743787271812, 54.8161537994206], [9.4573085512498, 54.8154065532489], [9.45710646534516, 54.814616070597], [9.456717750847, 54.8139093365088], [9.4563934338766, 54.813586882932], [9.45577648082094, 54.8131554296348], [9.45512263395655, 54.8127236689014], [9.45505668683493, 54.8124033329216], [9.45472278484044, 54.8107561227691], [9.45436982149396, 54.8090626390392], [9.45408512471918, 54.8086044118068], [9.45272991999335, 54.8075485227669], [9.45219139884514, 54.8074015067503], [9.4510346409487, 54.8070857011373], [9.44505378355119, 54.8054526225319], [9.44377911458721, 54.8051381245439], [9.44342407882736, 54.8048692404712], [9.44300991056562, 54.8043339278834], [9.44110691228123, 54.8015257633651], [9.44044070819177, 54.800551443488], [9.43954932430838, 54.8006959839271], [9.43973423981467, 54.8011914097092], [9.44179443518897, 54.8043906321498], [9.44201745992456, 54.804572622909], [9.44175207962333, 54.8046653949274], [9.44004295125196, 54.8046321776195], [9.4398954649503, 54.8045363662748], [9.43877031337885, 54.8033582047067], [9.43834082704486, 54.8030033704328], [9.43699791845867, 54.8011289010372], [9.43673183567488, 54.8005882204723], [9.43657110371435, 54.8001830333381], [9.43654822811563, 54.7997401561617], [9.43688664594103, 54.7967626843828], [9.43748039191965, 54.7934988559165], [9.43713514330481, 54.792678773244], [9.43692600832015, 54.7912142165798], [9.43682115606187, 54.7901495720481], [9.43669717867122, 54.7887440921784], [9.4360151480631, 54.7891922238195], [9.43568080679245, 54.7894119014878], [9.43493643579856, 54.7899269911512], [9.43480183762629, 54.7904330936831], [9.43464745495818, 54.7917280511419], [9.43460541829448, 54.7934043683489], [9.43416764679863, 54.7943306179965], [9.43378321546773, 54.7950741461632], [9.43315956367501, 54.7956325040648], [9.43206735063006, 54.7963982702348], [9.43073261850955, 54.7980778120762], [9.43010259586482, 54.7979175317104], [9.42891616661459, 54.7995137609532], [9.42851147557908, 54.800088025453], [9.42872546236461, 54.8012733472425], [9.42909997270149, 54.8018964374967], [9.4292767306067, 54.8021191710051], [9.42957274256079, 54.8024921723203], [9.43011974290698, 54.8030462617089], [9.43057914942812, 54.8032051066529], [9.43096930482399, 54.8032083841805], [9.43103399879579, 54.803200162401], [9.43138526252979, 54.8031555201647], [9.43190088382477, 54.8030189524855], [9.43248717648146, 54.8029816029776], [9.43316746705287, 54.8030859339112], [9.43425770829576, 54.8033768565089], [9.435333154285921, 54.805231573], [9.43619516595447, 54.8067916817687], [9.43698425238822, 54.8067982740362], [9.4366531558324, 54.8082011729484], [9.43776845108857, 54.8083093162949], [9.43755819680305, 54.8089134039586], [9.43720830389819, 54.8092486276586], [9.43655123208196, 54.8091867827157], [9.43636710799141, 54.8097206394576], [9.43587365082942, 54.8104361170012], [9.43550909921579, 54.810547042088], [9.43498121681547, 54.8105121440383], [9.43356984492652, 54.8103735232327], [9.43310970807507, 54.8104889642454], [9.43267222583675, 54.8108842002582], [9.43279850291453, 54.8110942084226], [9.43226639124588, 54.8113356977741], [9.43209986515177, 54.8114112706732], [9.4315661068712, 54.811710717824], [9.43065437165504, 54.8126528313849], [9.43021136477774, 54.8131464530755], [9.43001140698202, 54.8133692519197], [9.42974020020785, 54.8136898928965], [9.42930169658507, 54.8141230978633], [9.42866108549808, 54.8147445535761], [9.42850011907725, 54.8149287659437], [9.42805142137263, 54.8154422488644], [9.42730587717899, 54.8163097522029], [9.42705407850655, 54.8171624170461], [9.42680995234491, 54.8177112225383], [9.42629888011854, 54.8184097371596], [9.42598703715417, 54.8190339487393], [9.42587828441092, 54.8194319301876], [9.4258028986906, 54.8198111982936], [9.42559018751279, 54.8204172504559], [9.4253302222561, 54.8209149849518], [9.42531361420313, 54.8209467811392], [9.42490787842694, 54.8213802441682], [9.42417131109793, 54.8218868917314], [9.42347820246576, 54.8224915464673], [9.42333027149801, 54.8226205943784], [9.42293404888071, 54.8232229781175], [9.42475058072575, 54.8234006885366], [9.42760922387738, 54.8236802943259], [9.433401878833, 54.8242466680906], [9.440347428031, 54.8249253995686], [9.44342934654818, 54.8252264403052], [9.447420454456, 54.8256161740458], [9.43752275018025, 54.827965930251], [9.44363243227772, 54.8304307459143], [9.4492951926399, 54.8327659345094], [9.46028757403941, 54.8333407281332], [9.46306653662534, 54.8315865549029], [9.48292630497539, 54.8370771385797], [9.48989532266019, 54.8259243731887], [9.49055041638619, 54.8248756736174], [9.49194956489906, 54.8226356715221]]]}


#geofaktor= Werteübersicht  1 = ohne weitere Struktur Gewässer innerhalb des Gebiets (z.B. Gemeinde)
#                           2 = mit Struktur Gewässer 
#                           3 = ohne Struktur Land 
#                           4 = mit Struktur Land --> Von Interesse


df_gem_kreis_gefiltert = df_gem_kreis_extrahiert[df_gem_kreis_parsed["GF"] == 4.0]

df_gem_kreis_gefiltert.count()



# Den RS mit Nullen auffüllen sodass dieser 12 stellig wird (für bessere Zusammenführbarkeit mit anderen Datensätzen)
df_gem_kreis_gefiltert["RS"] = df_gem_kreis_gefiltert["RS"].apply(lambda x: x.ljust(12, "0"))
df_gem_kreis_gefiltert["RS_ALT"] = df_gem_kreis_gefiltert["RS_ALT"].apply(lambda x: x.ljust(12, "0"))
df_gem_kreis_gefiltert["RAU_RS"] = df_gem_kreis_gefiltert["RAU_RS"].apply(lambda x: x.ljust(12, "0"))


dateipfad_curated = pfad +  "\\2_curated\\daten\\"
dateiname_neu = "GEOD_KREIS_GEM_GRENZEN.csv"


## Datei abspeichern
df_gem_kreis_gefiltert.to_csv(dateipfad_curated + dateiname_neu, sep=";", header=True, index=False, encoding="ISO-8859-1")