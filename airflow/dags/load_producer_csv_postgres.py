## CHARGEMENT DES LIBRAIRIES ##
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine
import joblib

import warnings
warnings.filterwarnings("ignore")


## DEFINITION DES PARAMETRES DU DAG ##
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
}

dag = DAG(
    'load_producer_csv_postgres',
    default_args=default_args,
    description='DAG pour chargement des données de producer, mise à jour des dates, application du modèle de ML et sauvegarde des données dans la base de données Postgres',
    schedule_interval=None,
)


## DEFINITION DES FONCTIONS ##

# Database connection
DBHOST= 'host.docker.internal'
DBUSER= 'user'
DBPASS= 'password'
DBPORT= "5432"
DBNAME= 'fraud'
engine = create_engine(f"postgresql+psycopg2://{DBUSER}:{DBPASS}@{DBHOST}:{DBPORT}/{DBNAME}")

# Fonctions
def load_data_producer():
    data = pd.read_csv('src/producer.csv')
    print('Les données ont été chargées')
    return data

def apply_new_dates(data):
    data['trans_time'] = pd.to_datetime(data['trans_time'])
    date_now = datetime.now()
    last_date = data['trans_time'].iloc[-1]
    data['time_difference'] = last_date - data['trans_time']
    data['trans_time'] = data.apply(lambda x: date_now - x['time_difference'], axis = 1)
    data.drop(columns = ['time_difference'], inplace = True)
    print(f"Les dates ont été mises à jour à partir de {date_now.strftime('%Y-%m-%d %H:%M:%S')}")
    return data

def save_data_producer(data):
    data['dob'] = pd.to_datetime(data['dob'])
    data.to_sql('producer', con=engine, if_exists='replace', index=False)
    print('Producer sauvegardée dans la base de données')
    
def apply_ml(data):
    preprocessor = joblib.load('models/preprocessor.pkl')
    model = joblib.load('models/model.pkl')
    data['dob'] = pd.to_datetime(data['dob'])
    data_processed = data.drop(['trans_num', 'is_fraud'], axis=1)
    data_processed = preprocessor.transform(data)
    predictions = model.predict(data_processed)
    data_fraud = data.copy()
    data_fraud['prediction'] = predictions
    data_fraud = data_fraud.loc[:,['trans_num', 'prediction']]
    print('Les prédictions ont été effectuées')
    return data_fraud

def save_data_fraud(data_fraud):
    data_fraud.to_sql('predictions', con=engine, if_exists='replace', index=False)
    print('Prédictions sauvegardées dans la base de données')


## DEFINITION DES TACHES ##
with dag:
    load_data_producer_task = PythonOperator(
        task_id='load_data_producer',
        python_callable=load_data_producer,
    )

    apply_new_dates_task = PythonOperator(
        task_id='apply_new_dates',
        python_callable=apply_new_dates,
        op_args=[load_data_producer_task.output],
    )

    save_data_producer_task = PythonOperator(
        task_id='save_data_producer',
        python_callable=save_data_producer,
        op_args=[apply_new_dates_task.output],
    )

    apply_ml_task = PythonOperator(
        task_id='apply_ml',
        python_callable=apply_ml,
        op_args=[apply_new_dates_task.output],
    )

    save_data_fraud_task = PythonOperator(
        task_id='save_data_fraud',
        python_callable=save_data_fraud,
        op_args=[apply_ml_task.output],
    )

    load_data_producer_task >> apply_new_dates_task
    apply_new_dates_task >> [save_data_producer_task, apply_ml_task]
    apply_ml_task >> save_data_fraud_task