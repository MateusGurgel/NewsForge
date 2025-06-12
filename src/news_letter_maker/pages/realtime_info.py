import streamlit as st
from kafka import KafkaConsumer
import json
import time
from datetime import datetime

st.title("Informação em tempo real")

KAFKA_TOPIC = 'info-news'
KAFKA_SERVERS = 'localhost:9093'

if "messages" not in st.session_state:
    st.session_state.messages = []

if "processed_files" not in st.session_state:
    st.session_state.processed_files = 0

if "processed_lines" not in st.session_state:
    st.session_state.processed_lines = 0

if "consumer" not in st.session_state:
    try:
        st.session_state.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=2000,
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        st.success("Conectado ao Kafka!")
    except Exception as e:
        st.error(f"Erro ao conectar ao Kafka: {e}")
        st.stop()

def consume_messages():
    consumer = st.session_state.consumer
    new_messages = []

    try:
        msg_pack = consumer.poll(timeout_ms=3000, max_records=10)

        for tp, messages in msg_pack.items():
            for message in messages:
                if message.value:
                    try:
                        msg_data = json.loads(message.value)
                    except json.JSONDecodeError:
                        msg_data = message.value

                    new_messages.append({
                        'timestamp': datetime.fromtimestamp(message.timestamp/1000),
                        'partition': message.partition,
                        'offset': message.offset,
                        'value': msg_data
                    })

        return new_messages
    except Exception as e:
        st.error(f"Erro ao consumir mensagens: {e}")
        return []

# Interface do usuário
st.sidebar.header("Controles")

# Botão para atualizar manualmente
if st.sidebar.button("🔄 Atualizar Mensagens"):
    new_messages = consume_messages()
    if new_messages:
        st.session_state.messages.extend(new_messages)
        st.session_state.processed_files = new_messages[-1]['value']['files_read']
        st.session_state.processed_lines = new_messages[-1]['value']['lines_read']
        st.sidebar.success(f"Recebidas {len(new_messages)} novas mensagens!")
    else:
        st.sidebar.info("Nenhuma mensagem nova encontrada")

auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (5s)", value=False)
if auto_refresh:
    time.sleep(5)
    st.rerun()

if st.sidebar.button("🗑️ Limpar Mensagens"):
    st.session_state.messages = []
    st.session_state.processed_files = 0
    st.session_state.processed_lines = 0

with st.sidebar.expander("⚙️ Configurações Kafka"):
    st.code(f"""
Tópico: {KAFKA_TOPIC}
Servidor: {KAFKA_SERVERS}
    """)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Mensagens Recebidas", len(st.session_state.messages))
with col2:
    st.metric("Arquivos Processados", st.session_state.processed_files)
with col3:
    st.metric("Linhas Processadas", st.session_state.processed_lines)

st.subheader("📨 Mensagens Recebidas")

if st.session_state.messages:
    for i, msg in enumerate(reversed(st.session_state.messages[-10:])):  # Últimas 10
        with st.expander(f"Mensagem {len(st.session_state.messages) - i} - {msg['timestamp'].strftime('%H:%M:%S')}"):
            st.json(msg)
else:
    st.info("Nenhuma mensagem recebida ainda. Clique em 'Atualizar Mensagens' ou envie mensagens para o tópico Kafka.")

if st.session_state.messages:
    st.subheader("📊 Estatísticas")

    messages_per_minute = {}
    for msg in st.session_state.messages:
        minute_key = msg['timestamp'].strftime('%H:%M')
        messages_per_minute[minute_key] = messages_per_minute.get(minute_key, 0) + 1

    if messages_per_minute:
        st.bar_chart(messages_per_minute)

st.sidebar.markdown("---")
if "consumer" in st.session_state:
    st.sidebar.success("🟢 Conectado ao Kafka")
else:
    st.sidebar.error("🔴 Desconectado do Kafka")