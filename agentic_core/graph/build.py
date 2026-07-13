import os
from dotenv import load_dotenv
from typing import TypedDict, Annotated
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_ollama import ChatOllama

load_dotenv()

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]

def get_llm():
    return ChatOllama(
        model=os.getenv("LLM_MODEL", "llama3.2:3b"),
        base_url=os.getenv("LLM_BASE_URL", "http://localhost:11434"),
    )

def make_brain1_node(llm):
    def brain1_node(state: AgentState):
        response = llm.invoke(state["messages"])
        return {"messages": [response]}
    return brain1_node

def build_graph(checkpointer=None, llm=None):
    llm = llm or get_llm()
    graph = StateGraph(AgentState)
    graph.add_node("brain1", make_brain1_node(llm))
    graph.add_edge(START, "brain1")
    graph.add_edge("brain1", END)
    return graph.compile(checkpointer=checkpointer)
