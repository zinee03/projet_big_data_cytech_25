import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# 1. Configuration de la page
st.set_page_config(page_title="Projet Big Data - Taxi NYC", layout="wide")

st.title(" Dashboard Analyse Taxi NYC")
st.markdown("Ce tableau de bord est connecté directement au Data Warehouse PostgreSQL (`taxi_warehouse`).")

# 2. Connexion à la Base de Données
# Port 5432, User admin, Mdp toto, Base taxi_warehouse
DB_URI = "postgresql+psycopg2://admin:toto@localhost:5432/taxi_warehouse"


@st.cache_data(ttl=600)  # Mise en cache 10 min pour la performance
def load_data():
    try:
        engine = create_engine(DB_URI)
        # On lit les données. LIMIT 50000 pour que ça reste rapide.
        query = """
            SELECT 
                passenger_count, 
                trip_distance, 
                total_amount, 
                payment_type_id,
                pickup_datetime
            FROM fact_trips 
            LIMIT 50000
        """
        with engine.connect() as conn:
            # CORRECTION ICI : On utilise 'data' au lieu de 'df' pour éviter le conflit de nom
            data = pd.read_sql(query, conn)
        return data
    except Exception as e:
        st.error(f" Erreur de connexion : {e}")
        return pd.DataFrame()


# Chargement
with st.spinner('Chargement des données depuis le Warehouse...'):
    df = load_data()

if not df.empty:
    # 3. KPIs (Indicateurs Clés)
    st.header("Indicateurs Globaux")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    kpi1.metric("Nombre de courses", f"{len(df):,}")
    kpi2.metric("Chiffre d'Affaires ($)", f"{df['total_amount'].sum():,.2f} $")
    kpi3.metric("Distance Moyenne", f"{df['trip_distance'].mean():.2f} miles")
    kpi4.metric("Prix Moyen / Course", f"{df['total_amount'].mean():.2f} $")

    st.markdown("---")

    # 4. Visualisations Graphiques
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribution des Prix")
        # On filtre les outliers (> 100$) pour le graphique
        fig_hist = px.histogram(df[df['total_amount'] < 100], x="total_amount",
                                nbins=30,
                                title="Répartition du montant payé",
                                color_discrete_sequence=['#F0C05A'])
        st.plotly_chart(fig_hist, use_container_width=True)

    with col2:
        st.subheader("Relation Distance vs Prix")
        fig_scatter = px.scatter(df[df['trip_distance'] < 20], x="trip_distance", y="total_amount",
                                 title="Prix selon la distance",
                                 opacity=0.3)
        st.plotly_chart(fig_scatter, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Courses par passagers")
        df_pass = df['passenger_count'].value_counts().reset_index()
        df_pass.columns = ['Passagers', 'Nombre de courses']
        fig_bar = px.bar(df_pass, x='Passagers', y='Nombre de courses',
                         color='Nombre de courses')
        st.plotly_chart(fig_bar, use_container_width=True)

    with col4:
        st.subheader("Aperçu des données")
        st.dataframe(df.head(100), use_container_width=True)

else:
    st.warning(" La table 'fact_trips' semble vide ou inaccessible. Relance l'exercice 2 si besoin.")
