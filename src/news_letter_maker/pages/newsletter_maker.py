import streamlit as st
from datetime import datetime, timedelta

from news_letter_maker.lib.gemini import generate
from news_letter_maker.lib.news import get_news

st.title("Newsletter maker")

today = datetime.today()
seven_weeks_ago = today - timedelta(days=7)

start_date = st.date_input("Data de início", value=seven_weeks_ago)
end_date = st.date_input("Data de fim", value=today)

pressed = st.button("Gerar newsletter")

if pressed:

    tab1, tab2 = st.tabs(["Noticias Disponíveis", "Newsletter"])

    with tab2:
        with st.spinner("Carregando noticias..."):
            st.title("Noticias escolhidas")
            st.write("As noticias escolhidas para o newsletter são:")
            news = get_news(start_date, end_date)
            st.write(news)

    with tab1:
        with st.spinner("Gerando newsletter..."):

            prompt = f"""
            Você é um jornalista carismático e habilidoso, sua tarefa é fazer uma news letter somente com as 7
            noticiais mais relevantes e que chamam mais atenção.
            
            Escreva em formato markdown, de uma forma muito organizada e bonita.
            
            Siga o formato abaixo:
            
            Não responda nada além da newsletter
            
            para cabeçalho
            *  Seja extremamente curto.
            *  O titulo deve ser um mini compilado de noticias separados por '/'
            *  Não use bullet points.
            *  O titulo deve ser médio. use ##
            
            para noticias
            *  Adicione um título, um resumo e a fonte da notícia.
            *  Não adicione marcadores como: 'titulo:', 'resumo:'
            *  A fonte deve ser escrito em minusculo, após o resumo.
            *  A fonte deve ser bem pequena 
            *  Use separadores entre as noticias
            *  Não use bullet points.
            
            para fontes
            *  A fonte deve ser escrita em minusculo.
            *  A fonte tem que estar uma linha abaixo do resumo
            
            <News>
                {str(news)}
            </News>
            """

            result = generate(prompt)

            st.markdown(result, unsafe_allow_html=True)