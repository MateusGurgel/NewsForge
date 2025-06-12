from datetime import datetime
from sqlalchemy import text

from news_letter_maker.utils.db import get_db_engine


def get_news(start_date: datetime, end_date: datetime):
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            start_date_str = start_date.strftime("%Y-%m-%d 00:00:00")
            end_date_str = end_date.strftime("%Y-%m-%d 23:59:59")

            query = text("""
                SELECT id, title, origin, resume, transcription, collected_at 
                FROM news 
                WHERE collected_at BETWEEN :start_date AND :end_date 
                ORDER BY collected_at DESC
            """)

            result = connection.execute(query, {"start_date": start_date_str, "end_date": end_date_str})

            news_list = []
            for row in result:
                news_list.append({
                    "id": row.id,
                    "title": row.title,
                    "origin": row.origin,
                    "description": row.resume,
                    "transcription": row.transcription,
                    "date": row.collected_at
                })

            return news_list
    except Exception as e:
        print(f"Database error: {e}")
        raise ValueError("Invalid database connection.")
