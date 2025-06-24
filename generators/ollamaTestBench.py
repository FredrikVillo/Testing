import requests
import time
import json

def query_ollama(prompt, model_name):
    url = "http://localhost:11434/api/generate"
    headers = {"Content-Type": "application/json"}
    data = {"model": model_name, "prompt": prompt, "stream": False}
    start = time.time()
    response = requests.post(url, headers=headers, json=data)
    elapsed = time.time() - start
    result = response.json()
    return result.get("response", ""), elapsed

def main():
    prompt = input("Enter the prompt to use for benchmarking: ")
    models = input("Enter comma-separated model names (e.g. gemma3:1b,gemma3:4b,llama3:8b): ")
    model_list = [m.strip() for m in models.split(",") if m.strip()]
    if not model_list:
        print("No models specified.")
        return
    print(f"\nBenchmarking {len(model_list)} models...")
    results = []
    for model in model_list:
        print(f"\n--- Running model: {model} ---")
        output, elapsed = query_ollama(prompt, model)
        print(f"Model: {model}\nTime: {elapsed:.2f} seconds\nOutput (truncated):\n{output[:300]}{'...' if len(output) > 300 else ''}")
        results.append((model, elapsed))
    print("\n=== Benchmark Results ===")
    for model, elapsed in results:
        print(f"{model}: {elapsed:.2f} seconds")
    fastest = min(results, key=lambda x: x[1])
    print(f"\nFastest: {fastest[0]} ({fastest[1]:.2f} seconds)")

if __name__ == "__main__":
    main()
