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
    'apply_ml_current_data',
    default_args=default_args,
    description='DAG pour application du machine learning sur les données actuelles Postgres',
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
def load_data_sql():
    data = pd.read_sql('SELECT * FROM producer', con=engine)
    print('Les données ont été chargées')
    return data
    
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
    print(data_fraud['prediction'].value_counts())
    print('Prédictions sauvegardées dans la base de données')

## DEFINITION DES TACHES ##
with dag:
    load_data_sql_task = PythonOperator(
        task_id='load_data_producer',
        python_callable=load_data_sql,
    )

    apply_ml_task = PythonOperator(
        task_id='apply_ml',
        python_callable=apply_ml,
        op_args=[load_data_sql_task.output],
    )

    save_data_fraud_task = PythonOperator(
        task_id='save_data_fraud',
        python_callable=save_data_fraud,
        op_args=[apply_ml_task.output],
    )

    load_data_sql_task >> apply_ml_task >> save_data_fraud_task

    