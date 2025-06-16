### Datenbank mit neuer Spalte

def laden_in_db(df):
    erneuerbar = "Erneuerbare"
    konventionell = "Konventionelle"

    conn = sqlite3.connect('strom_datenbank.db')
    #with conn:
    #df.to_sql('strom', conn, index=False)
        #df.to_sql('strom', conn, if_exists='replace', index=False)

    try:
        df_alt = pd.read_sql("SELECT * FROM strom", conn)
    except Exception:
        df_alt = pd.DataFrame()

    if not df_alt.empty:
        df = df[~df["Timestamp"].isin(df_alt["Timestamp"])]

# Nur wenn es neue Daten gibt, schreiben
    if not df.empty:
        df.to_sql('strom', conn, if_exists='append', index=False)
        print(f"{len(df)} neue Zeilen wurden ergänzt.")
    else:
        print("Keine neuen Daten.")

    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(strom)")
    spalten = [row[1] for row in cursor.fetchall()]
    if erneuerbar not in spalten:
        cursor.execute(f"ALTER TABLE strom ADD COLUMN {erneuerbar} FLOAT")
    if konventionell not in spalten:
        cursor.execute (f"ALTER TABLE strom ADD COLUMN {konventionell} FLOAT")
    cursor.execute(f"""UPDATE strom SET {erneuerbar} = "Biomasse"+ "Wasserkraft"+"Wind Offshore"+"Wind Onshore"+"Photovoltaik"+"Sonstige Erneuerbare"+"Pumpspeicher" WHERE {erneuerbar} IS NULL""")
    cursor.execute(f"""UPDATE strom SET  {konventionell}= "Braunkohle" + "Steinkohle" + "Erdgas"+ "sonstige Konventionelle" WHERE {konventionell} IS NULL""")
    conn.commit()


    conn.close()


#### CSV einlesen neu

import pandas as pd
import os
from glob import glob

def csv_einlesen_ordner(pfad_zum_ordner):

    dateien = glob(os.path.join(pfad_zum_ordner, "*.csv"))


    gruppen = {}
    for datei in dateien:
        name = os.path.basename(datei)
        teile = name.split("_")
        if len(teile) >= 5:
            key = "_".join(teile[2:4])  # z. B. "202501130000_202501270000"
            gruppen.setdefault(key, []).append(datei)
        else:
            print(f"⚠️  Datei {name} entspricht nicht dem erwarteten Namensschema – übersprungen.")

    gesamt_df = pd.DataFrame()

    for gruppe, dateiliste in gruppen.items():
        if len(dateiliste) != 2:
            print(f"⚠️  Warnung: Gruppe {gruppe} enthält {len(dateiliste)} Dateien (statt 2). Übersprungen.")
            continue

        # Einlesen beider Dateien in der Gruppe
        df1 = pd.read_csv(dateiliste[0], sep=";", decimal=",", thousands=".", parse_dates=["Datum von", "Datum bis"], dayfirst=True)
        df2 = pd.read_csv(dateiliste[1], sep=";", decimal=",", thousands=".", parse_dates=["Datum von", "Datum bis"], dayfirst=True)

        # Zusammenführen über gemeinsame Spalten
        df = pd.merge(df1, df2, on=["Datum von", "Datum bis"], how="outer")

        gesamt_df = pd.concat([gesamt_df, df], ignore_index=True)

    return gesamt_df


