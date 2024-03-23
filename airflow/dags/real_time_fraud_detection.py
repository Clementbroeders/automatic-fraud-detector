## CHARGEMENT DES LIBRAIRIES ##
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
import pandas as pd
from sqlalchemy import create_engine
import joblib

import warnings
warnings.filterwarnings("ignore")


## DEFINITION DES PARAMETRES DU DAG ##
now = datetime.now()
start_date = now - timedelta(minutes=5)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 5,
}

dag = DAG(
    'real_time_fraud_detection',
    default_args=default_args,
    description='DAG pour détection de fraude en temps réel, récupère les données de l\'API toutes les 15 secondes, applique le modèle de ML et sauvegarde les données dans la base de données Postgres',
    schedule_interval=timedelta(seconds=15),
)


## DEFINITION DES FONCTIONS ##

# API connection
url = 'https://real-time-payments-api.herokuapp.com/current-transactions'
headers = {'accept': 'application/json'}

# Database connection
DBHOST= 'host.docker.internal'
DBUSER= 'user'
DBPASS= 'password'
DBPORT= "5432"
DBNAME= 'fraud'
engine = create_engine(f"postgresql+psycopg2://{DBUSER}:{DBPASS}@{DBHOST}:{DBPORT}/{DBNAME}")

# Fonctions
def get_data_api():
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_data = response.json()
        data = pd.read_json(json_data, orient='split')
        data.rename(columns={'current_time': 'trans_time'}, inplace=True)
        data['trans_time'] = pd.to_datetime(data['trans_time'])
        data['dob'] = pd.to_datetime(data['dob'], format='%Y-%m-%d')
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Time : {date} - Data reçue de l\'API - Transaction : {data['trans_num'].iloc[0]} - Fraude : {data['is_fraud'].iloc[0]}")
        return data
    else:
        print("Erreur lors de la requête :", response.status_code)
        raise Exception("Erreur lors de la requête vers l'API")


def apply_ml(data):
    preprocessor = joblib.load('models/preprocessor.pkl')
    model = joblib.load('models/model.pkl')
    
    data_processed = data.drop(['trans_num', 'is_fraud'], axis=1)
    data_processed = preprocessor.transform(data)
    predictions = model.predict(data_processed)
    data_fraud = data.copy()
    data_fraud['prediction'] = predictions
    data_fraud = data_fraud.loc[:,['trans_num', 'prediction']]
    
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if predictions[0] == 1:
        print(f"Time : {date} - /!\ Fraude détectée /!\ - Transaction : {data_fraud['trans_num'].iloc[0]} - Prediction : {data_fraud['prediction'].iloc[0]}")
    else:
        print(f"Time : {date} - Pas de fraude détectée - Transaction : {data_fraud['trans_num'].iloc[0]}")
    return data_fraud

def save_data_api(data):
    data.to_sql('producer', con=engine, if_exists='append', index=False)
    print('Data sauvegardée dans la base de données')

def save_data_fraud(data_fraud):
    data_fraud.to_sql('predictions', con=engine, if_exists='append', index=False)
    print('Prédictions sauvegardées dans la base de données')


## DEFINITION DES TACHES ##
with dag:
    get_data_task = PythonOperator(
        task_id='get_data_api_task',
        python_callable=get_data_api,
    )

    save_data_api_task = PythonOperator(
        task_id='save_data_api_task',
        python_callable=save_data_api,
        op_args=[get_data_task.output],
    )

    apply_ml_task = PythonOperator(
        task_id='apply_ml_task',
        python_callable=apply_ml,
        op_args=[get_data_task.output],
    )

    save_data_fraud_task = PythonOperator(
        task_id='save_data_fraud_task',
        python_callable=save_data_fraud,
        op_args=[apply_ml_task.output],
    )

    get_data_task >> apply_ml_task
    apply_ml_task >> [save_data_api_task, save_data_fraud_task]
