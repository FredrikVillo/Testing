"""Customer chatbot using LangGraph, LM Studio local LLMs, and SQL Server lookup."""
from typing import TypedDict, List
from langgraph.graph import StateGraph
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from sql_tools import run_query

# ---- LLM helpers --------------------------------------------------------
def make_llm(model: str, temperature: float = 0.3, max_tokens: int = 256):
    return ChatOpenAI(
        base_url="http://localhost:1234/v1",
        openai_***REMOVED***="lm-studio",
        model_name=model,
        temperature=temperature,
        model_kwargs={"max_tokens": max_tokens},
    )

router_llm  = make_llm("phi-2", temperature=0.1)
faq_llm     = make_llm("phi-2", temperature=0.0, max_tokens=120)
planner_llm = make_llm("mistral-7b-instruct-v0.3", temperature=0.2)
data_llm    = make_llm("mistral-7b-instruct-v0.3", temperature=0.25)

faq_prompt = ChatPromptTemplate.from_messages([
    ("system", "Answer in ≤2 sentences. No introductions or storytelling."),
    ("user", "{q}")
])
planner_prompt = ChatPromptTemplate.from_messages([
    ("user", "You are a strategic assistant. Plan the steps:\n\n{q}")
])
data_prompt = ChatPromptTemplate.from_messages([
    ("user", "Using the following database result, answer the user's question.\n\nUser: {q}\n\nResult: {rows}")
])

# ---- State definition ---------------------------------------------------
class ChatState(TypedDict):
    q: str                 # user query
    intent: str            # router chooses: faq | plan | data
    rows: List[dict]       # db rows (for data intent)

# ---- Router -------------------------------------------------------------
KEYWORDS_FAQ  = ["password", "pricing", "hours", "support", "contact"]
KEYWORDS_PLAN = ["how do i", "steps", "procedure", "process"]

def router_node(state: ChatState) -> ChatState:
    q = state["q"]
    q_lower = q.lower()
    # Heuristic for FAQ/PLAN
    if any(k in q_lower for k in KEYWORDS_FAQ):
        print("[ROUTER] Heuristic matched FAQ")
        intent = "faq"
    elif any(k in q_lower for k in KEYWORDS_PLAN):
        print("[ROUTER] Heuristic matched PLAN")
        intent = "plan"
    else:
        prompt = (
            "You are a router. Decide the intent for the following user query. "
            "Reply with only one word: faq, plan, or data.\n"
            f"User query: {q}"
        )
        intent = router_llm.invoke(prompt).content.strip().lower()
        print(f"[ROUTER] LLM intent: {intent}")
        if intent not in {"faq", "plan", "data"}:
            intent = "data"
    new_state = {**state, "intent": intent}
    print(f"[ROUTER NODE] Input: {state} | Output: {new_state}")
    return new_state

# ---- FAQ handler --------------------------------------------------------
def faq_node(state: ChatState) -> ChatState:
    print(f"[FAQ NODE] Input: {state}")
    answer = faq_llm.invoke(faq_prompt.format(q=state["q"])).content.strip()
    print("[FAQ] →", answer)
    return {**state, "answer": answer}

# ---- Planner handler ----------------------------------------------------
def planner_node(state: ChatState) -> ChatState:
    print(f"[PLANNER NODE] Input: {state}")
    answer = planner_llm.invoke(planner_prompt.format(q=state["q"])).content.strip()
    print("[PLANNER] →", answer)
    return {**state, "answer": answer}

# ---- Data (SQL) handler --------------------------------------------------
def data_node(state: ChatState) -> ChatState:
    print(f"[DATA NODE] Input: {state}")
    sql = "SELECT name FROM sys.tables"
    tables = [r['name'] for r in run_query(sql, 100)]
    results = []
    for tbl in tables[:5]:
        try:
            rows = run_query(f"SELECT TOP 5 * FROM {tbl}")
            if rows:
                results.extend(rows)
        except Exception:
            continue
    state["rows"] = results[:20]
    answer = data_llm.invoke(data_prompt.format(q=state["q"], rows=state["rows"])).content.strip()
    print("[DATA] →", answer)
    return {**state, "answer": answer}

# ---- Build LangGraph ----------------------------------------------------
builder = StateGraph(state_schema=ChatState)
builder.add_node("router", router_node)
builder.add_node("faq", faq_node)
builder.add_node("planner", planner_node)
builder.add_node("data", data_node)
builder.set_entry_point("router")
builder.add_conditional_edges(
    "router",
    {
        "faq": faq_node,
        "plan": planner_node,
        "data": data_node,
    },
    lambda s: s["intent"]
)

chat_graph = builder.compile()

# ---- Optional: Graph image ----------------------------------------------
try:
    import networkx as nx, pydot
    G = nx.DiGraph()
    G.add_edges_from([("router", "faq"), ("router", "planner"), ("router", "data")])
    pydot_graph = nx.nx_pydot.to_pydot(G)
    pydot_graph.write_png("chat_graph.png")
    print("✔ Saved chatbot graph to 'chat_graph.png'")
except Exception:
    pass

def chat_response(user_msg: str) -> str:
    print(f"[CHATBOT] Received user message: {user_msg}")
    try:
        final_state = chat_graph.invoke({"q": user_msg, "rows": []})
        print(f"[CHATBOT] Final state: {final_state}")
        answer = final_state.get("answer", None)
        if not answer:
            print("[CHATBOT] No answer found in final state!")
            return "Sorry, I couldn't find an answer."
        return str(answer)
    except Exception as e:
        print(f"[CHATBOT ERROR] Exception during chat_graph.invoke: {e}")
        return f"Sorry, something went wrong: {e}"

if __name__ == "__main__":
    print(chat_response("What are your opening hours?"))
