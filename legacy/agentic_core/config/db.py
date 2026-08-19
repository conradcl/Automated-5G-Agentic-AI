import os
import psycopg
from dotenv import load_dotenv
from langgraph.checkpoint.postgres import PostgresSaver

load_dotenv()

def get_checkpointer():
    db_uri = os.getenv("POSTGRES_URI")
    if not db_uri:
        raise ValueError("POSTGRES_URI not set in environment")
    conn = psycopg.connect(db_uri, autocommit=True)
    saver = PostgresSaver(conn)
    saver.setup()
    return saver
