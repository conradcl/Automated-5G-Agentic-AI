# agentic_core/graph/build.py
from typing import TypedDict, Annotated
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_ollama import ChatOllama

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]

llm = ChatOllama(model="llama3.2:3b", base_url="http://localhost:11434")

def brain1_node(state: AgentState):
    print(">>> brain1_node EXECUTED <<<")
    response = llm.invoke(state["messages"])
    return {"messages": [response]}

def build_graph(checkpointer=None):
    graph = StateGraph(AgentState)
    graph.add_node("brain1", brain1_node)
    graph.add_edge(START, "brain1")
    graph.add_edge("brain1", END)
    return graph.compile(checkpointer=checkpointer)
