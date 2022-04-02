from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import psycopg2

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:8000",
    'http://localhost:8081',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/")
def read_root():
    return {'news': [{'title': 'some title 1', 'company': 'Nviia'}]}

@app.get("/api/news_list")
def read_root():
    conn = psycopg2.connect(dbname='airflow', user='airflow',
                            password='airflow', host='localhost')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM user_space.vw_company_news limit 20')
    records = cursor.fetchall()
    cursor.close()
    conn.close()

    news_list = []
    for n in records:
        description = n[1]
        if description is not None and len(description) > 120:
            description = description[0:115] + '...'

        news_list.append({
            'title': n[0],
            'description': description,
            'company': n[2],
            'publish_timestamp': n[3],
            'image_url': n[4]
        })

    return {'news': news_list}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}