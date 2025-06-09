"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import glob
import zipfile
import pandas as pd
import io
import os

def clean_campaign_data():

    #Cargar archivos
    def load_input(input_directory):
        files = glob.glob(f"{input_directory}/*.zip")
        dfs = []

        for file in files:
            with zipfile.ZipFile(file, 'r') as fzip:        
                for name in fzip.namelist():
                    with fzip.open(name) as fcsv:
                        df = pd.read_csv(io.TextIOWrapper(fcsv, encoding='utf-8'))
                        dfs.append(df)
        return pd.concat(dfs, ignore_index=True)

    df = load_input("files/input")

    # Definimos los archivos de salida
    client_column = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    campaign_column = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "month", "day"]].copy()
    economics_column = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    # Limpiamos los datos de client.csv
    client_column.loc[:, 'job'] = client_column['job'].str.replace(r'[.]', '', regex=True).str.replace(r'-', '_', regex=True)
    client_column.loc[:, 'education'] = client_column['education'].str.replace(r'[.]', '_', regex=True).replace('unknown', pd.NA)
    client_column.loc[:, 'credit_default'] = client_column['credit_default'].apply(lambda x: 1 if x == "yes" else 0)
    client_column.loc[:, "mortgage"] = client_column["mortgage"].apply(lambda x: 1 if x == "yes" else 0)


    # Limpiamos los datos de campaign.csv
    campaign_column.loc[:, "previous_outcome"] = campaign_column["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign_column.loc[:, "campaign_outcome"] = campaign_column["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    campaign_column.loc[:, "month"] = pd.to_datetime(campaign_column["month"], format='%b', errors='coerce').dt.month.astype(str).str.zfill(2)
    campaign_column.loc[:, "last_contact_date"] = pd.to_datetime("2022" + "-" + campaign_column["month"] + "-" + campaign_column["day"].astype(str), errors='coerce')
    campaign_column = campaign_column.drop(columns=["month", "day"])

    #Creamos carpeta de salida
    def output_directory(output_directory):
        if os.path.exists(output_directory):
            for file in glob.glob(f"{output_directory}/*"):
                os.remove(file)
            os.rmdir(output_directory)
        os.makedirs(output_directory)

    # Guardamos los archivos de salida
    def save_output(output_directory):
        client_column.to_csv(output_directory + "/client.csv", index=False)
        campaign_column.to_csv(output_directory + "/campaign.csv", index=False)
        economics_column.to_csv(output_directory + "/economics.csv", index=False)

    
    output_directory("files/output")
    save_output("files/output") 
    
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """


    return




if __name__ == "__main__":
    clean_campaign_data()
