# AUTOMATIC FRAUD DETECTOR

<p align="center">
    <img src="img/image.jpg" alt="Image" width="50%" height="50%">
</p>


## Introduction

Dans un paysage où la fraude financière représente une menace croissante pour les institutions financières, l'intégration de technologies avancées telles que l'intelligence artificielle (IA) et le traitement des données en temps réel est devenue indispensable. Notre projet repose sur une architecture technique sophistiquée pour détecter et prévenir les paiements frauduleux de manière proactive.

Au cœur de notre infrastructure se trouve Apache Airflow, un outil puissant de gestion des flux de travail qui orchestre le processus d'Extraction, Transformation et Chargement (ETL). Airflow nous permet de collecter les données de paiement en temps réel, de les prétraiter et de les stocker dans une base de données relationnelle, assurant ainsi leur disponibilité pour l'analyse et la modélisation.

our le stockage et la gestion des données, nous utilisons une base de données relationnelle PostgreSQL. Cette base de données offre une grande fiabilité et une flexibilité pour stocker les données transactionnelles et les résultats des modèles de machine learning.

En ce qui concerne le machine learning, nous exploitons les bibliothèques scikit-learn et XGboost pour développer et entraîner nos modèles de détection de fraude. Ces bibliothèques fournissent des outils avancés pour la création de modèles prédictifs précis et évolutifs, permettant ainsi une détection efficace des fraudes en temps réel.

En combinant ces technologies de pointe, notre infrastructure permet une détection proactive des fraudes, fournissant ainsi aux institutions financières les moyens de sécuriser leurs opérations et de protéger leurs clients contre les menaces croissantes de fraude.


## Docker Compose

Pour lancer l'application complète, vous pouvez utiliser la commande : 

`docker compose up --build`

Les tableau de bord est accessible dans le Streamlit. Les données y sont accessibles, y compris que dans la base de données PostgreSQL, requêtable via PGAdmin.



## Airflow (DAGs)

Pour charger les données du producer (données fictives sur 7 jours), il est nécessaire de se connecter à **Airflow** et de lancer le DAG `save_producer_postgres`. 

Ce DAG va écraser les données actuelles dans la base de données PostgreSQL pour les remplacer par des données fictives à jour. 

/!\ A n'utiliser qu'une fois au lancement de l'application, au risque de perdre les données récupérées de l'API.

Une fois les données chargées, il faut activer le DAG `fraud_detection_dag` afin de lancer la récupération des données (toutes les 15 secondes) et le traitement machine learning.


## Machine-Learning

Un modèle de machine learning (avec son preprocesseur) est déjà disponible dans le repository (sous dossier `./models`) et vous pouvez l'utiliser comme tel (déjà configuré par défaut).

Si vous souhaitez entrainer vous-même votre modèle, vous pouvez utiliser le DAG `create_ml_model`. A noter qu'il faut modifier dans le code : `n_iter`, `cv` pour l'adapter à vos souhaits.

Une fois le nouveau modèle créé, vous pouvez lancer le DAG `apply_ml_current_data` pour mettre à jour la table de données `predictions` (cette opération n'écrase pas les données de la table `producer`)


## Applications

Le projet comporte plusieurs applications :

- Airflow : http://localhost:8080

    Port 8080

    Compte : airflow

    Mot de passe : airflow

<br>

- Streamlit : http://localhost:8501

    Port 8501

<br>

- Base de données Postgres
    
    Port 5432

    Compte : user

    Mot de passe : password

<br>

- PGAdmin http://localhost:5050

    Port : 5050

    Mot de passe : password

<br>