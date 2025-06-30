import subprocess
import sys
import os
import json
import time
import pyodbc

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


def run_pipeline(schema_file: str):
    python_exec = sys.executable
    steps = [
        ("🔍 Steg 1: Kjører databaseAnalyser...", [python_exec, "databaseAnalyser.py", schema_file]),
        ("🧪 Steg 2: Kjører qualityControl...", [python_exec, "qualityControl.py", schema_file.replace(".json", "_analyzed.json")]),
        ("🤖 Steg 3: Kjører aiDataGenerator...", [python_exec, "aiDataGenerator.py", schema_file.replace(".json", "_analyzed_qualitychecked.json")]),
        ("🧹 Steg 4: Kjører qualityControl_2 på cleanData.json...", [python_exec, "qualityControl_2.py"]),
        ("🗄️ Steg 5: Kjører testDatabaseCreator på cleanDataQcChecked.json...", [python_exec, "testDatabaseCreator.py"]),
    ]
    total_start = time.time()
    if TQDM_AVAILABLE:
        bar = tqdm(total=len(steps), desc="Pipeline Progress", ncols=80)
    else:
        print("Pipeline Progress:")
    for i, (desc, cmd) in enumerate(steps):
        print(f"\n{desc}")
        step_start = time.time()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate()
        print(stdout)
        if proc.returncode != 0:
            print(stderr)
            sys.exit(1)
        step_elapsed = time.time() - step_start
        print(f"Step {i+1}/{len(steps)} complete. Tid brukt: {step_elapsed:.1f} sekunder.")
        if TQDM_AVAILABLE:
            bar.update(1)
        time.sleep(0.5)
    if TQDM_AVAILABLE:
        bar.close()
    total_elapsed = time.time() - total_start
    print(f"\n✅ Hele pipelinen er ferdig! Total tid: {total_elapsed:.1f} sekunder.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_pipeline.py <schema_file.json>")
        sys.exit(1)

    input_schema = sys.argv[1]
    run_pipeline(input_schema)
