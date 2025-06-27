"""
Minimal agentic multi‑model pipeline for LM Studio.
Requirements:
    pip install -r requirements.txt
Usage:
    python main.py
LM Studio must be running on http://localhost:1234 with the models:
    - mistralai/mistral-7b-instruct-v0.3
    - codellama-7b-instruct
    - phi-2
"""

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains.router import MultiPromptChain

# Helper to create an LLM wrapper for LM Studio local server
def make_llm(model_name: str, temperature: float = 0.3):
    return ChatOpenAI(
        base_url="http://localhost:1234/v1",
        openai_***REMOVED***="lm-studio",   # any non‑empty string
        model_name=model_name,
        temperature=temperature,
    )

# Instantiate models
planner_llm = make_llm("mistral-7b-instruct-v0.3", temperature=0.2)
coder_llm   = make_llm("codellama-7b-instruct", temperature=0.25)
router_llm  = make_llm("phi-2", temperature=0.1)

# Prompt templates
planner_prompt = ChatPromptTemplate.from_messages([
    ("user", "You are a strategic assistant that breaks down complex tasks.\n\n{input}")
])
coder_prompt = ChatPromptTemplate.from_messages([
    ("user", "You are a coding assistant. Produce clear, correct code.\n\n{input}")
])

destination_chains = {
    "planner": planner_prompt | planner_llm,
    "coder":   coder_prompt   | coder_llm,
}


def route_query(text: str) -> str:
    if any(word in text.lower() for word in ["code", "python", "sql", "generate function"]):
        return "coder"
    prompt = f"Decide if this task is best for 'planner' or 'coder'. Only reply with one word.\nTask: {text}"
    response = router_llm.invoke(prompt).content.strip().lower()
    return response if response in destination_chains else "planner"



def agentic_run(user_input: str):
    route = route_query(user_input)
    print(f"[Router]→{route}")
    result = destination_chains[route].invoke({"input": user_input})
    print(f"[{route.upper()}]:\n{result.content}\n")


if __name__ == "__main__":
    # Simple demo tasks
    tasks = [
        "Outline a plan to anonymize HR data for testing.",
        "Write Python code that replaces names and emails using Faker."
    ]
    for t in tasks:
        agentic_run(t)
