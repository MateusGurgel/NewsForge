import streamlit as st

pg = st.navigation([
    st.Page("pages/newsletter_maker.py", title="Newsletter maker", icon="🗞️"),
    st.Page("pages/realtime_info.py", title="Informação em tempo real", icon="📊"),
])
pg.run()