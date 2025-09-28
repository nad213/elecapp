import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

st.title("Consommation")

# 1. Configurer DuckDB pour S3
conn = duckdb.connect()
conn.execute(f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_region='{st.secrets["AWS_S3_REGION"]}'; 
    SET s3_access_key_id='{st.secrets["AWS_ACCESS_KEY_ID"]}';
    SET s3_secret_access_key='{st.secrets["AWS_SECRET_ACCESS_KEY"]}';
""")

# 2. Récupérer les dates min et max du dataset
s3_path_puissance = st.secrets["s3_paths"]["puissance"]
query_min_max = f"""
    SELECT MIN(date_heure) as min_date, MAX(date_heure) as max_date
    FROM read_parquet('{s3_path_puissance}');
"""
min_max_dates = conn.execute(query_min_max).fetchdf()
min_date = pd.to_datetime(min_max_dates['min_date'].iloc[0]).date()
max_date = pd.to_datetime(min_max_dates['max_date'].iloc[0]).date()

# 3. Sélection de la plage de dates
default_end = max_date
default_start = default_end - timedelta(days=90)
dates_range = st.slider(
    "Sélectionnez une plage de dates :",
    min_value=min_date,
    max_value=max_date,
    value=(default_start, default_end),
    format="YYYY-MM-DD"
)

# 4. Charger les données pour la plage sélectionnée
start_str = dates_range[0].strftime("%Y-%m-%d")
end_str = dates_range[1].strftime("%Y-%m-%d")
query = f"""
    SELECT date_heure, consommation
    FROM read_parquet('{s3_path_puissance}')
    WHERE date_heure BETWEEN '{start_str}' AND '{end_str} 23:59:59'
    ORDER BY date_heure;
"""
df = conn.execute(query).fetchdf()

# 5. Graphique : Consommation par date
fig1 = px.line(df, x='date_heure', y='consommation', title="Consommation par date")
st.plotly_chart(fig1, use_container_width=True)

# 6. Charger les données annuelles depuis S3
s3_path_annuel = st.secrets["s3_paths"]["annuel"]
df_annuel = conn.execute(f"SELECT * FROM read_parquet('{s3_path_annuel}')").fetchdf()

# 7. Graphique : Consommation annuelle en barres
fig2 = px.bar(
    df_annuel,
    x='annee',
    y='consommation_annuelle',
    title="Consommation annuelle",
    labels={'annee': 'Année', 'consommation_annuelle': 'Consommation annuelle (MWh)'}
)
st.plotly_chart(fig2, use_container_width=True)

# 8. Charger les données mensuelles depuis S3
s3_path_mensuel = st.secrets["s3_paths"]["mensuel"]
df_mensuel = conn.execute(f"SELECT * FROM read_parquet('{s3_path_mensuel}')").fetchdf()

# Convertir 'annee_mois' en chaîne pour un affichage plus lisible
df_mensuel['annee_mois_str'] = df_mensuel['annee_mois'].astype(str)

# 9. Graphique : Consommation mensuelle en barres
fig3 = px.bar(
    df_mensuel,
    x='annee_mois_str',
    y='consommation_mensuelle',
    title="Consommation mensuelle",
    labels={'annee_mois_str': 'Mois', 'consommation_mensuelle': 'Consommation mensuelle (MWh)'}
)
st.plotly_chart(fig3, use_container_width=True)
