import requests
import json
import time

def benchmark_models(models, prompt, num_trials=3):
    url = "http://localhost:1234/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    results = []
    for model in models:
        print(f"\n=== Benchmarking model: {model} ===")
        times = []
        valid_json_count = 0
        for i in range(num_trials):
            data = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 512,
                "temperature": 0.6
            }
            start = time.time()
            try:
                response = requests.post(url, headers=headers, data=json.dumps(data))
                elapsed = time.time() - start
                times.append(elapsed)
                result = response.json()
                content = result["choices"][0]["message"]["content"]
                # Check if output is valid JSON
                try:
                    json.loads(content)
                    valid_json_count += 1
                except Exception:
                    pass
                print(f"Trial {i+1}: {elapsed:.2f}s | JSON valid: {valid_json_count}/{i+1}")
            except Exception as e:
                print(f"Trial {i+1}: Error: {e}")
        avg_time = sum(times) / len(times) if times else float('nan')
        results.append({
            "model": model,
            "avg_time": avg_time,
            "valid_json": valid_json_count,
            "total": num_trials
        })
    print("\n=== Benchmark Summary ===")
    for r in results:
        print(f"Model: {r['model']} | Avg Time: {r['avg_time']:.2f}s | JSON Valid: {r['valid_json']}/{r['total']}")

if __name__ == "__main__":
    # List your models here
    models = [
        "google/gemma-3-12b"
    ]
    # Use a simple prompt for benchmarking
    prompt = "Generate a JSON object with a random name and age."
    benchmark_models(models, prompt, num_trials=3)
