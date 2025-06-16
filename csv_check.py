
import pandas as pd
import sqlite3

def csv_einlesen(dateipfad1, dateipfad2, dateipfad3, dateipfad4):

    df1a = pd.read_csv(dateipfad1, sep=";", decimal=",", thousands=".", parse_dates=["Datum von", "Datum bis"], dayfirst=True)
    df1b= pd.read_csv(dateipfad2, sep=";", decimal=",", thousands=".", parse_dates=["Datum von", "Datum bis"], dayfirst=True)
    df1 =pd.merge(df1a, df1b, on=["Datum von", "Datum bis"], how="outer")

    df2a = pd.read_csv(dateipfad3, sep=";", decimal=",", thousands=".", parse_dates=["Datum von", "Datum bis"],
                       dayfirst=True)
    df2b = pd.read_csv(dateipfad4, sep=";", decimal=",", thousands=".", parse_dates=["Datum von", "Datum bis"],
                       dayfirst=True)
    df2 = pd.merge(df2a, df2b, on=["Datum von", "Datum bis"], how="outer")

    df=pd.concat([df1, df2], axis=0, ignore_index=True)

    return df

def transform(df):
    df.drop(columns=["Kernenergie [MWh] Berechnete Auflösungen", "Netzlast inkl. Pumpspeicher [MWh] Berechnete Auflösungen", "Datum bis"], inplace=True)
    df.columns= df.columns.str.replace(" [MWh] Berechnete Auflösungen", "")
    df.rename(columns={'Datum von': 'Timestamp'}, inplace=True)



    #beide Datensätze kombinieren

    df["Monat"]=df["Timestamp"].dt.strftime("%B")
    #Definition von Nacht: 22 Uhr bis 6 Uhr
    df["Stunde"] = df["Timestamp"].dt.hour
    df["Tageszeit"]= df["Stunde"].apply(lambda x: "Nacht" if x>=22 or x<6 else "Tag")
    df.drop(columns=["Stunde"], inplace=True)

    return df

def laden_in_db(df):
    conn = sqlite3.connect('strom_datenbank.db')
    df.to_sql('strom', conn, if_exists='replace', index=False)

def panda_check(df):
    print(df.head(10))

    print(df.info())
    print(df.describe())


df= transform(csv_einlesen("Dez_2024/erzeugung1.csv", "Dez_2024/stromverbrauch1.csv", "Dez_2024/erzeugung2.csv", "Dez_2024/stromverbrauch2.csv"))
laden_in_db(df)
panda_check(df)
