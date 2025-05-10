from modello_base import ModelloBase
import pandas as pd
import pymysql

class DatasetCleaner(ModelloBase):

    def __init__(self, dataset_path):
        self.dataframe = pd.read_csv(dataset_path)
        self.dataframe_sistemato = self.sistemazione()

    # Metodo di sistemazione dataframe
    def sistemazione(self):
        # Copia del dataframe
        df_sistemato = self.dataframe.copy()
        # Drop variabile pm25tmean2
        df_sistemato = df_sistemato.drop(["pm25tmean2"], axis=1)
        # Drop variabile con un solo valori univoco
        colonna_unico_valore = df_sistemato.nunique()[df_sistemato.nunique() < 2].index
        df_sistemato = df_sistemato.drop(colonna_unico_valore, axis=1)
        # Rimappatura variabile Unnamed: 0
        df_sistemato = df_sistemato.rename(columns={"Unnamed: 0":"id_air_pollution"})
        # Conversione tipo variabile date
        df_sistemato["date"] = pd.to_datetime(df_sistemato["date"])
        # Drop valori nan
        df_sistemato = df_sistemato.dropna()
        # Gestione valori duplicati
        if df_sistemato.duplicated().any():
            df_sistemato = df_sistemato.drop_duplicates().reset_index(drop=True)

        return df_sistemato

# Funzione per stabile una connessione con chicago_db
def getconnection():
    return pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="",
        database="chicago_db"
    )

# Funzione per creare la tabella air_pollution
def creazione_tabella():
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                sql = ("CREATE TABLE IF NOT EXISTS air_pollution("
                       "id_air_pollution INT PRIMARY KEY,"
                       "tmpd FLOAT NOT NULL,"
                       "dptp FLOAT NOT NULL,"
                       "date DATE NOT NULL,"
                       "pm10tmean2 FLOAT NOT NULL,"
                       "o3tmean2 FLOAT NOT NULL,"
                       "no2tmean2 FLOAT NOT NULL"
                       ");")
                cursor.execute(sql)
                connection.commit()
                print("Tabella air_pollution creata con successo")
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

# Funzione per caricare i dati nel db
def load(df):
    try:
        connection = getconnection()
        try:
            with connection.cursor() as cursor:
                valori = [(
                    row["id_air_pollution"],
                    row["tmpd"],
                    row["dptp"],
                    row["date"],
                    row["pm10tmean2"],
                    row["o3tmean2"],
                    row["no2tmean2"]
                ) for _, row in df.iterrows()]

                sql = ("INSERT INTO air_pollution(id_air_pollution, tmpd, dptp, date, pm10tmean2, o3tmean2, no2tmean2) "
                       "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                cursor.executemany(sql, valori)
                connection.commit()
                print("Dati caricati con successo")
        finally:
            connection.close()
    except Exception as e:
        print(e)
        return None

modello = DatasetCleaner("../Dataset/dataset.csv")
# Passo 1. Analisi generale del dataframe
#modello.analisi_generali(modello.dataframe)
# Risultati:
# Osservazioni= 6940; Variabili= 9; Tipi= object, int e float; Valori nan= presenti
# Variabile pm25tmean2 con più del 50% valori nan -> Drop
# Variabile Unnamed -> Rimappatura con id_air_pollution
# Passo 2. Analisi valori univoci
#modello.analisi_valori_univoci(modello.dataframe)
# Risultati:
# Variabile city un solo valore univoco -> Drop
# Valori nan camufatti= non presenti
# Passo 3. Drop variabile pm25tmean2
# Passo 4. Drop variabile con un solo valori univoco
# Passo 5. Rimappatura variabile Unnamed: 0
# Passo 6. Conversione tipo variabile date -> object diventa di tipo date
# Passo 7. Strategia gestione valori nan
# Passo 7.1. Prova drop:
# Valori prima del drop= 6940; Valori dopo del drop= 6695
# Percentuale dataset perso = [(6940 - 6695) / 6940 ] * 100 -> 3.53%
# Percentuale di dataset perso accettabile (meno del 5%)
# Passo 8. Gestione valori duplicati
# Passo 9. Analisi outliers
#modello.individuazione_outliers(modello.dataframe_sistemato, ["id_air_pollution", "date"])
# Risultati:
# tmpd= 0.01%
# dptp= 0.1%
# pm10tmean2= 3.5%
# o3tmean2= 0.64%
# no2tmean2= 1.38%
# Outliers inferiore al 10/15% -> lascio così
# Passo 10. Stabilisco una connessione con il chicago_db
# Passo 11. Creazione tabella air_pollution
# creazione_tabella()
# Passo 12. Load dei dati
#load(modello.dataframe_sistemato)