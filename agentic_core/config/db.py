import os
from langgraph.checkpoint.postgres import PostgresSaver

DB_URI = os.getenv("POSTGRES_URI", "postgresql://user:pass@localhost:5432/agentic_ai")

def get_checkpointer():
    with PostgresSaver.from_conn_string(DB_URI) as saver:
        saver.setup()  # creates tables on first run
        return saver
