import tkinter as tk
from tkinter import filedialog, scrolledtext, ttk, messagebox
import threading
import subprocess
import sys
import os

def run_pipeline(schema_file, progress_callback, log_callback):
    python_exec = sys.executable
    steps = [
        ("Kjører databaseAnalyser...", [python_exec, "databaseAnalyser.py", schema_file]),
        ("Kjører qualityControl...", [python_exec, "qualityControl.py", schema_file.replace(".json", "_analyzed.json")]),
        ("Kjører aiDataGenerator...", [python_exec, "aiDataGenerator.py", schema_file.replace(".json", "_analyzed_qualitychecked.json")]),
        ("Kjører qualityControl_2...", [python_exec, "qualityControl_2.py"]),
        ("Kjører testDatabaseCreator...", [python_exec, "testDatabaseCreator.py"]),
    ]
    for i, (desc, cmd) in enumerate(steps):
        log_callback(f"\n{desc}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate()
        log_callback(stdout)
        if proc.returncode != 0:
            log_callback(stderr)
            messagebox.showerror("Pipeline Error", f"Step failed: {desc}\n{stderr}")
            return
        progress_callback(i+1, len(steps))
    log_callback("\n✅ Hele pipelinen er ferdig!")
    messagebox.showinfo("Pipeline", "Hele pipelinen er ferdig!")

def start_pipeline_thread(schema_file, progressbar, log_area, start_btn):
    def progress_callback(step, total):
        progressbar['value'] = (step/total)*100
        progressbar.update()
    def log_callback(text):
        log_area.insert(tk.END, text + "\n")
        log_area.see(tk.END)
    def run():
        start_btn.config(state=tk.DISABLED)
        progress_callback(0, 5)
        log_area.delete(1.0, tk.END)
        run_pipeline(schema_file, progress_callback, log_callback)
        start_btn.config(state=tk.NORMAL)
    threading.Thread(target=run, daemon=True).start()

def main():
    root = tk.Tk()
    root.title("TestData Pipeline GUI")
    root.geometry("700x500")

    frm = tk.Frame(root)
    frm.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    tk.Label(frm, text="Schema file:").pack(anchor=tk.W)
    schema_var = tk.StringVar()
    entry = tk.Entry(frm, textvariable=schema_var, width=60)
    entry.pack(side=tk.LEFT, padx=(0,5), pady=(0,10))
    def browse():
        f = filedialog.askopenfilename(filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
        if f:
            schema_var.set(f)
    tk.Button(frm, text="Browse", command=browse).pack(side=tk.LEFT, pady=(0,10))

    progressbar = ttk.Progressbar(root, orient="horizontal", length=600, mode="determinate")
    progressbar.pack(pady=10)

    log_area = scrolledtext.ScrolledText(root, height=20, width=80)
    log_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    def start():
        schema_file = schema_var.get()
        if not schema_file or not os.path.isfile(schema_file):
            messagebox.showerror("Error", "Please select a valid schema file.")
            return
        start_pipeline_thread(schema_file, progressbar, log_area, start_btn)

    start_btn = tk.Button(root, text="Start Pipeline", command=start)
    start_btn.pack(pady=10)

    root.mainloop()

if __name__ == "__main__":
    main()
