#This program should run all the other programs in the aiConversions folder

import subprocess
import sys
import os
import json
import datetime
import shutil
from token_logger import update_pipeline_status, reset_pipeline_status, extract_token_usage

# List of generator scripts in order
GENERATOR_SCRIPTS = [
    "generators/organizationAiDataGenerator.py",
    "generators/employeeAiDataGenerator.py",
    "generators/scaleAiDataGenerator.py",
    "generators/accessCatalystDataGenerator_ai.py",
    "generators/userProfileDataGeneratorAi.py",
    "generators/userprofileHistoryAiGenerator.py"
]

PYTHON = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".venv", "Scripts", "python.exe")
STATUS_PATH = "pipeline_status.json"


def get_output_dirs(dry_run=False):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_dir = os.path.abspath(os.path.dirname(__file__))
    output_root = os.path.join(base_dir, "output")
    if dry_run:
        timestamp_dir = os.path.join(output_root, "dryRun")
    else:
        timestamp_dir = os.path.join(output_root, timestamp)
    latest_dir = os.path.join(output_root, "latest")
    os.makedirs(timestamp_dir, exist_ok=True)
    os.makedirs(latest_dir, exist_ok=True)
    return timestamp_dir, latest_dir


def run_script(script_path, *script_args):
    print(f"\n▶ Running {script_path} ...")
    try:
        result = subprocess.run(
            [PYTHON, script_path, *script_args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True
        )
        output = (result.stdout or "") + (result.stderr or "")
        print(output)
        tokens = extract_token_usage(output)
        update_pipeline_status(script_path, "success", tokens, STATUS_PATH)
        return True, tokens
    except subprocess.CalledProcessError as e:
        err_output = (e.stdout or "") + (e.stderr or "")
        print(f"❌ Error in {script_path}: {err_output}")
        update_pipeline_status(script_path, "failed", 0, STATUS_PATH)
        return False, 0


def main():
    reset_pipeline_status(STATUS_PATH)
    dry_run = "--dry-run" in sys.argv
    timestamp_dir, latest_dir = get_output_dirs(dry_run)
    total_tokens = 0
    for script in GENERATOR_SCRIPTS:
        args = [timestamp_dir]
        if dry_run:
            args.append("--dry-run")
        ok, tokens = run_script(script, *args)
        total_tokens += tokens
        if not ok:
            print(f"Pipeline stopped at {script} due to error.")
            break
        print(f"✅ {script} finished. Tokens used: {tokens}")
    # After all scripts, copy timestamp_dir to latest_dir (overwrite)
    if os.path.exists(latest_dir):
        shutil.rmtree(latest_dir)
    shutil.copytree(timestamp_dir, latest_dir)
    print(f"\nPipeline complete. Total tokens used: {total_tokens}")
    print(f"Output saved to: {timestamp_dir}\nAlso copied to: {latest_dir}")
    # Print status summary
    with open(STATUS_PATH) as f:
        status = json.load(f)
        print(json.dumps(status, indent=2))

if __name__ == "__main__":
    main()
