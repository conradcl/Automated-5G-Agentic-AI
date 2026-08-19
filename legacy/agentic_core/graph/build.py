import os
from dotenv import load_dotenv
from typing import TypedDict, Annotated
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_ollama import ChatOllama
from langchain_deepseek import ChatDeepSeek

load_dotenv()

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]

def get_llm():
    provider = os.getenv("LLM_PROVIDER", "ollama")

    if provider == "deepseek":
        print(f">>> Using DeepSeek: {os.getenv('DEEPSEEK_MODEL', 'deepseek-v4-flash')}")
        return ChatDeepSeek(
            model=os.getenv("DEEPSEEK_MODEL", "deepseek-v4-flash"),
            api_key=os.getenv("DEEPSEEK_API_KEY"),
        )

    print(f">>> Using Ollama: {os.getenv('LLM_MODEL', 'llama3.2:3b')}")
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
