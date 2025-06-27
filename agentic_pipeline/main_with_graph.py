"""
Minimal agentic multi-model pipeline for LM Studio.
Requirements:
    pip install -r requirements.txt
Usage:
    python main_with_graph.py
LM Studio must be running on http://localhost:1234 with the models:
    - mistralai/mistral-7b-instruct-v0.3
    - codellama-7b-instruct
    - phi-2
"""

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains.router import MultiPromptChain
from langgraph.graph import StateGraph
from typing import TypedDict
import networkx as nx
import pydot


# ──────────────────────────────────────────────────────────────
# Helper: wrap a local LM Studio model in ChatOpenAI-compatible LLM
# ──────────────────────────────────────────────────────────────
def make_llm(model_name: str, temperature: float = 0.3):
    return ChatOpenAI(
        base_url="http://localhost:1234/v1",
        openai_***REMOVED***="lm-studio",  # any non-empty string
        model_name=model_name,
        temperature=temperature,
    )

# ──────────────────────────────────────────────────────────────
# Instantiate 3 local models
# ──────────────────────────────────────────────────────────────
planner_llm = make_llm("mistral-7b-instruct-v0.3", temperature=0.2)
coder_llm   = make_llm("codellama-7b-instruct",   temperature=0.25)
router_llm  = make_llm("phi-2",                  temperature=0.10)

# ──────────────────────────────────────────────────────────────
# Prompt templates
# ──────────────────────────────────────────────────────────────
planner_prompt = ChatPromptTemplate.from_messages([
    ("user", "You are a strategic assistant that breaks down complex tasks.\n\n{input}")
])
coder_prompt = ChatPromptTemplate.from_messages([
    ("user", "You are a coding assistant. Produce clear, correct code.\n\n{input}")
])

destination_chains = {
    "planner": planner_prompt | planner_llm,
    "coder"  : coder_prompt   | coder_llm,
}

# ──────────────────────────────────────────────────────────────
# Heuristic+LLM routing
# ──────────────────────────────────────────────────────────────
def route_query(text: str) -> str:
    # Fast keyword heuristic
    if any(word in text.lower() for word in ["code", "python", "sql", "generate function"]):
        return "coder"
    # Otherwise ask router LLM
    prompt = (
        "Decide if this task is best for 'planner' or 'coder'. "
        "Only reply with one word.\n"
        f"Task: {text}"
    )
    response = router_llm.invoke(prompt).content.strip().lower()
    return response if response in destination_chains else "planner"

# ──────────────────────────────────────────────────────────────
# Run a single task through the pipeline
# ──────────────────────────────────────────────────────────────
def agentic_run(user_input: str):
    route  = route_query(user_input)
    print(f"[Router] → {route}")
    result = destination_chains[route].invoke({"input": user_input})
    print(f"[{route.upper()} OUTPUT]\n{result.content}\n")

# ──────────────────────────────────────────────────────────────
# Script entry point
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # ── LangGraph visualisation scaffold ──────────────────────
    class AgentState(TypedDict):
        input: str

    builder = StateGraph(state_schema=AgentState)
    builder.add_node("router",  lambda state: {"input": state["input"]})
    builder.add_node("planner", lambda state: state)
    builder.add_node("coder",   lambda state: state)
    builder.set_entry_point("router")
    builder.add_edge("router", "planner")
    builder.add_edge("router", "coder")

    graph = builder.compile()

    # --- Visualize and save the graph as PNG using pydot ---
    # Manually build a NetworkX graph for visualization
    G = nx.DiGraph()
    G.add_node("router")
    G.add_node("planner")
    G.add_node("coder")
    G.add_edge("router", "planner")
    G.add_edge("router", "coder")
    pydot_graph = nx.nx_pydot.to_pydot(G)
    pydot_graph.write_png("graph.png")
    print("✔ Saved LangGraph diagram to 'graph.png'")

    # --- Demo tasks ---
    demo_tasks = [
        "Outline a plan to anonymize HR data for testing.",
        "Write Python code that replaces names and emails using Faker."
    ]
    for task in demo_tasks:
        agentic_run(task)
