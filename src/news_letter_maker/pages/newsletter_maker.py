import streamlit as st
from datetime import datetime, timedelta

from news_letter_maker.lib.news import get_news

st.title("Newsletter maker")

today = datetime.today()
seven_weeks_ago = today - timedelta(days=7)

start_date = st.date_input("Data de início", value=seven_weeks_ago)
end_date = st.date_input("Data de fim", value=today)

pressed = st.button("Gerar newsletter")

if pressed:
    news = get_news(start_date, end_date)
    st.write(news)
    st.write("Aqui você pode criar um newsletter com as informações processadas.")