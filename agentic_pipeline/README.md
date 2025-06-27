# Agentic Pipeline with LM Studio (Minimal)

**Date generated:** 2025-06-26T06:00:29.071812 UTC

## What is this?

A tiny proof‑of‑concept showing how to orchestrate three local LLMs
("mistral‑7b‑instruct", "codellama‑7b‑instruct", "phi‑2") loaded in LM Studio
through one OpenAI‑compatible endpoint, then routed via LangChain.

## Quick start

```bash
# 1) start LM Studio, load the three models, click “Start Server”
#    (endpoint: http://localhost:1234)

# 2) clone / unzip this folder
cd agentic_pipeline

# 3) create a virtual env (optional but recommended)
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate

# 4) install deps
pip install -r requirements.txt

# 5) run
python main.py
```
