import streamlit as st

st.title("Informação em tempo real")

st.metric("Arquivos Processados", 42)

st.bar_chart({
    "Linhas Processados": 10000,
},
horizontal=True
)