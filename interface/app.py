import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

# Конфигурация postgres
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "fraud_db"),
    "user": os.getenv("DB_USER", "fraud_user"),
    "password": os.getenv("DB_PASSWORD", "fraud_pass")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")


def fetch_fraud_results(limit=10):
    """Fetch latest fraud transactions from DB"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT transaction_id, score, fraud_flag, timestamp
            FROM scoring_results
            WHERE fraud_flag = 1
            ORDER BY timestamp DESC
            LIMIT %s;
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {str(e)}")
        return pd.DataFrame()

def fetch_all_scores():
    """Fetch scores from the last 100 records for histogram"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT score 
            FROM scoring_results 
            ORDER BY timestamp DESC 
            LIMIT 100;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df['score'].values
    except Exception as e:
        st.error(f"Ошибка загрузки оценок: {str(e)}")
        return np.array([])
    
st.markdown("---")
st.subheader("📊 Результаты скоринга")

if st.button("Посмотреть результаты"):
    fraud_df = fetch_fraud_results(limit=10)
    
    if fraud_df.empty:
        st.info("Нет обнаруженных мошеннических транзакций.")
    else:
        st.write(f"Найдено **{len(fraud_df)}** мошеннических транзакций (показаны последние 10):")
        st.dataframe(fraud_df)

    scores = fetch_all_scores()
    
    if len(scores) > 0:
        st.markdown("### Распределение скоров (последние 100 записей)")
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.hist(scores, bins=30, color='skyblue', edgecolor='black', alpha=0.7)
        ax.set_xlabel('Score (вероятность)')
        ax.set_ylabel('Количество транзакций')
        ax.set_title('Гистограмма распределения скоров (последние 100)')
        ax.axvline(x=0.33, color='red', linestyle='--', label='Порог (0.33)')
        ax.legend()
        st.pyplot(fig)
    else:
        st.info("Нет данных для построения гистограммы.")