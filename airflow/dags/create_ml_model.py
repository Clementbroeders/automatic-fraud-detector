## CHARGEMENT DES LIBRAIRIES ##
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

from lightgbm import LGBMClassifier
from sklearn.metrics import make_scorer
from sklearn.model_selection import RandomizedSearchCV
from scipy.stats import randint, uniform
from sklearn.metrics import f1_score, recall_score


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
    'create_ml_model',
    default_args=default_args,
    description="DAG pour l'entraînement du modèle de ML sur les données de fraude, sauvegarde des données dans MLflow et dans le dossier models/",
    schedule_interval=None,
)


## DEFINITION DES FONCTIONS ##

# Fonctions
def import_dataset():
    try:
        data = pd.read_csv('src/fraudTest.csv', index_col=0)
        print('Le fichier existe déjà, il a été chargé')
    except FileNotFoundError:
        url_data = 'https://lead-program-assets.s3.eu-west-3.amazonaws.com/M05-Projects/fraudTest.csv'
        data = pd.read_csv(url_data, index_col=0)
        data.to_csv('src/fraudTest.csv')
        print("Le fichier n'existait pas, il a été téléchargé et sauvegardé dans le dossier src/")
    return data

def data_cleaning(data):
    data['trans_time'] = pd.to_datetime(data['trans_date_trans_time'])
    data['dob'] = pd.to_datetime(data['dob'])
    delete_columns = ['unix_time', 'trans_date_trans_time', 'trans_num']
    data = data.drop(delete_columns, axis=1)
    print('Les données ont été nettoyées')
    return data

def train_test_model_training(data):
    X = data.drop('is_fraud', axis=1)
    y = data['is_fraud']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    numeric_features = X.select_dtypes(include=['float', 'int']).columns
    datetime_features = X.select_dtypes(include=['datetime']).columns
    text_features = X.select_dtypes(include=['object']).columns
    
    numeric_transformer = StandardScaler()
    datetime_transformer = StandardScaler()
    text_transformer = OneHotEncoder(drop='first')
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('numeric', numeric_transformer, numeric_features),
            ('datetime', datetime_transformer, datetime_features),
            ('text', text_transformer, text_features)
        ])
    
    X_train = preprocessor.fit_transform(X_train)
    X_test = preprocessor.transform(X_test)
    print('Les données ont été prétraitées')

    model = LGBMClassifier()
    param_dist = {
        'n_estimators': randint(50, 1000),
        'max_depth': randint(3, 15),
        'num_leaves': randint(6, 50),
        'learning_rate': uniform(0.01, 0.3),
        'feature_fraction': uniform(0.5, 0.5),
        'bagging_fraction': uniform(0.5, 0.5),
        'min_child_samples': randint(20, 200),
        'lambda_l1': uniform(0, 1),
        'lambda_l2': uniform(0, 1),
        'min_gain_to_split': uniform(0, 1),
        'min_split_gain': uniform(0, 1)
    }
    f1_scorer = make_scorer(f1_score)
    random_search = RandomizedSearchCV(estimator=model, param_distributions=param_dist, scoring=f1_scorer, n_iter=5, cv=5)
    random_search.fit(X_train, y_train)
    print('Le modèle a été entraîné sur le random search, les meilleurs paramètres ont été trouvés')
    
    best_model = random_search.best_estimator_
    best_model.fit(X_train, y_train)
    print('Le modèle a été entraîné avec les meilleurs paramètres')
    
    train_accuracy = best_model.score(X_train, y_train)
    test_accuracy = best_model.score(X_test, y_test)
    y_train_pred = best_model.predict(X_train)
    y_test_pred = best_model.predict(X_test)
    train_recall = recall_score(y_train, y_train_pred, average='binary')
    test_recall = recall_score(y_test, y_test_pred, average='binary')
    train_f1 = f1_score(y_train, y_train_pred, average='binary')
    test_f1 = f1_score(y_test, y_test_pred, average='binary')
    print('Train accuracy:', train_accuracy)
    print('Test accuracy:', test_accuracy)
    print('Train recall:', train_recall)
    print('Test recall:', test_recall)
    print('Train f1 score:', train_f1)
    print('Test f1 score:', test_f1)
          
    joblib.dump(random_search.best_params_, 'models/best_params.pkl')
    print('Les meilleurs paramètres ont été sauvegardés dans le fichier models/best_params.pkl')
        
def full_dataset_model_training(data):
    X = data.drop('is_fraud', axis=1)
    y = data['is_fraud']

    numeric_features = X.select_dtypes(include=['float', 'int']).columns
    datetime_features = X.select_dtypes(include=['datetime']).columns
    text_features = X.select_dtypes(include=['object']).columns

    numeric_transformer = StandardScaler()
    datetime_transformer = StandardScaler()
    text_transformer = OneHotEncoder(drop='first')

    preprocessor = ColumnTransformer(
        transformers=[
            ('numeric', numeric_transformer, numeric_features),
            ('datetime', datetime_transformer, datetime_features),
            ('text', text_transformer, text_features)
        ])
    
    X = preprocessor.fit_transform(X)
    print('Preprocessing terminé')
    
    joblib.dump(preprocessor, 'models/preprocessor.pkl')
    print('Preprocessing enregistré au chemin /models/preprocessor.pkl')
    
    best_params = joblib.load('models/best_params.pkl')
    best_model = LGBMClassifier(**best_params)
    best_model.fit(X, y)
    print('Modélisation du best model terminée')
    
    accuracy = best_model.score(X, y)
    pred = best_model.predict(X)
    recall = recall_score(y, pred, average='binary')
    f1 = f1_score(y, pred, average='binary')
    
    print('Accuracy:', accuracy)
    print('Recall:', recall)
    print('F1 score:', f1)

    joblib.dump(best_model, 'models/model.pkl')
    print('Modèle enregistré au chemin /models/model.pkl')


## DEFINITION DES TACHES ##

with dag:
    import_dataset_task = PythonOperator(
        task_id='import_dataset',
        python_callable=import_dataset,
    )

    data_cleaning_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning,
        op_args=[import_dataset_task.output],
    )

    train_test_model_training_task = PythonOperator(
        task_id='train_test_model_training',
        python_callable=train_test_model_training,
        op_args=[data_cleaning_task.output],
    )

    full_dataset_model_training_task = PythonOperator(
        task_id='full_dataset_model_training',
        python_callable=full_dataset_model_training,
        op_args=[data_cleaning_task.output],
    )

    import_dataset_task >> \
        data_cleaning_task >> \
        train_test_model_training_task >> \
        full_dataset_model_training_task