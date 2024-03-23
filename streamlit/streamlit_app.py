### LIBRAIRIES ###
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

### CONFIGURATION ###
st.set_page_config(
    page_title="Automatic Fraud Detector Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


## HEADER ##
st.title("📊 Automatic Fraud Detector Dashboard 📊")


## CHARGEMENT DES DONNEES ##
@st.cache_data(ttl=60)

def load_data():
    DBHOST = os.environ.get('DBHOST')
    DBUSER = os.environ.get('DBUSER')
    DBPASS = os.environ.get('DBPASS')
    DBPORT = 5432
    DBNAME = os.environ.get('DBNAME')
    engine = create_engine(f"postgresql+psycopg2://{DBUSER}:{DBPASS}@{DBHOST}:{DBPORT}/{DBNAME}")
    query = '''
    SELECT producer.*, predictions.prediction
    FROM public.producer
    RIGHT JOIN public.predictions
    ON public.producer.trans_num = public.predictions.trans_num
    '''
    
    data = pd.read_sql_query(query, engine)
    data.dropna(subset=['trans_time'], inplace=True)
    data['trans_date'] = pd.to_datetime(data['trans_time']).dt.date
    return data

try:
    data = load_data()
except Exception as e:
    st.error("Une erreur s'est produite lors du chargement des données")
    st.write("Veuillez vous connecter à Airflow afin de lancer les DAG :")
    st.write("1)**save_producer_postgres**")
    st.write("2)**fraud_detection_dag**")
    Lien = "http://localhost:8080"
    st.write(f"Rendez-vous sur {Lien} pour activer les DAG")
    st.write(f'User : airflow - Password : airflow')

## SIDE BAR
st.sidebar.image('../img/image.jpg', use_column_width="auto")
st.sidebar.title("🔍 Filtres 🔍")
selection_fraude = st.sidebar.toggle(label = "Fraude uniquement")
if selection_fraude:
    data = data[data['prediction'] == 1]
    
min_date = data['trans_date'].min()
max_date = data['trans_date'].max()
date_selection = st.sidebar.date_input("Selectionnez les dates", value=(min_date, max_date), min_value=min_date, max_value=max_date, format="DD.MM.YYYY")
    
if len(date_selection) == 1:
    data = data[data['trans_date'] == date_selection[0]]
else:
    data = data[(data['trans_date'] >= date_selection[0]) & (data['trans_date'] <= date_selection[1])]

## APPLICATION ##
st.write("Ce dashboard a pour but de visualiser les données de fraudes détectées par le système automatique de détection de fraude.")
st.write("⬅️ Utilisez la barre latérale pour filtrer les données")

st.write('---')

affichage_data = st.toggle(label = "Afficher les données")
if affichage_data:
    st.dataframe(data)
    st.write('Nombre de lignes :', data.shape[0])
    
st.write("---")

st.subheader("📈 Analyse globale 📈")
if len(date_selection) == 1:
    st.write('Période filtrée :', date_selection[0].strftime('%d/%m/%Y'))
else:
    st.write('Période filtrée :', date_selection[0].strftime('%d/%m/%Y'), 'à', date_selection[1].strftime('%d/%m/%Y'))

columns = st.columns(3)
columns[0].metric('Nombre de fraudes :', data['prediction'].sum())
columns[1].metric('Nombre de transactions :', data.shape[0])
columns[2].metric('Taux de fraude :', f"{round(data['prediction'].sum() / data.shape[0] * 100, 2)} %")

fraud_count = data[data['prediction'] == 1].value_counts('trans_date')
fig = px.bar(fraud_count, 
            x=fraud_count.index, 
            y=fraud_count.values, 
            title='Nombre de fraudes par jour', 
            labels={'current_time': 'Date', 'prediction': 'Nombre de fraudes'},
            height = 600)
st.plotly_chart(fig, use_container_width = True)

fig_2 = px.scatter_mapbox(data[data['prediction'] == 1], 
                        lat="lat", 
                        lon="long", 
                        color="prediction", 
                        zoom=3.5, 
                        mapbox_style="carto-positron",
                        color_continuous_scale='reds', 
                        size_max=10,
                        size='prediction',
                        title = "Carte de localisation des fraudes",
                        center = {"lat": 39.8283, "lon": -98.5795},
                        height = 800)
st.plotly_chart(fig_2, use_container_width = True)