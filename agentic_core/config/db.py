import os
from dotenv import load_dotenv
from langgraph.checkpoint.postgres import PostgresSaver

load_dotenv()

def get_checkpointer():
    db_uri = os.getenv("POSTGRES_URI")
    if not db_uri:
        raise ValueError("POSTGRES_URI not set in environment")
    with PostgresSaver.from_conn_string(db_uri) as saver:
        saver.setup()
        return saver
