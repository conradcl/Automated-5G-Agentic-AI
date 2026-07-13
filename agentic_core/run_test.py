from graph.build import build_graph

app = build_graph()
print(app.get_graph().draw_mermaid())

result = app.invoke({"messages": [("user", "Is the testbed healthy?")]})
print(result["messages"][-1].content)
