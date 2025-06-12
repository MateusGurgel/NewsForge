from datetime import datetime


def get_news(start_date: datetime, end_date: datetime):
    return [
        {
            "id": 1,
            "title": "News 1",
            "description": "News 1 description",
            "date": datetime(2023, 1, 1),
        },
        {
            "id": 2,
            "title": "News 2",
            "description": "News 2 description",
            "date": datetime(2023, 1, 2),
        }
    ]