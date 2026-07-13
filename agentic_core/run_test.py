from graph.build import build_graph
from config.db import get_checkpointer

checkpointer = get_checkpointer()
app = build_graph(checkpointer=checkpointer)

# Fixed thread_id for local smoke-testing persistence across invocations.
# Replace with a dynamically generated id in real usage.
config = {"configurable": {"thread_id": "test-thread-1"}}

result1 = app.invoke({"messages": [("user", "My name is User.")]}, config=config)
print(result1["messages"][-1].content)

result2 = app.invoke({"messages": [("user", "What's my name?")]}, config=config)
print(result2["messages"][-1].content)
