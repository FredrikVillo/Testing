"""
offline_ai_anonymizer.py
------------------------------------------------------------
End-to-end test-data generator with offline AI text synthesis
• Local model: vikhr-gemma-2b-instruct (LM Studio)
• Async batched prompt execution with caching
• Faster DB inserts via fast_executemany
• Generates CSV files + writes to a clone database
------------------------------------------------------------
Requirements:
  pip install pyodbc Faker tqdm requests
------------------------------------------------------------
Run:
  python offline_ai_anonymizer.py
"""

import os
import sys
import csv
import json
import time
import random
import hashlib
import requests
import pyodbc
from faker import Faker
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ───────────────────────────── CONFIG ───────────────────────────── #

# DB connection (read from env OR use defaults)
SOURCE_DB = os.getenv("SOURCE_DB", "analyticsroutinedev")
DEST_DB   = os.getenv("DEST_DB",   "test_hris_synthetic")
SERVER    = os.getenv("DB_SERVER", "DESKTOP-R9S4CFK")
UID       = os.getenv("DB_UID",    "sa")
PWD       = os.getenv("DB_PWD",    "(catalystone123)")
DRIVER    = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

# LM Studio
LM_URL       = "http://localhost:1234/v1/completions"
MODEL_NAME   = "vikhr-gemma-2b-instruct"
MAX_TOKENS   = 128
TEMPERATURE  = 0.7
WORKERS      = 4          # concurrent threads
CACHE_FILE   = "prompt_cache.json"

OUTPUT_DIR   = "output_csvs"
BATCH_SIZE   = 500        # rows fetched from DB at a time (lowered to avoid MemoryError)

# Free-text columns that truly need AI realism
FREE_TEXT_FIELDS = {
    "description", "comment", "bio", "notes", "summary",
    "about", "details", "remarks", "feedback", "message",
    "text", "content"
}

# Simple PII keyword list for Faker replacement
PII_KEYWORDS = [
    'name','email','address','phone','ssn','guid','account','user','password',
    'secret','dob','birth','surname','firstname','lastname','city','zip',
    'postal','contact','mobile','credit','iban','bic','card','passport',
    'license','tax','vat','pin','token','profile'
]

fake = Faker()

# ──────────────────────── DB HELPERS ──────────────────────── #

def connect(db:str, autocommit=False):
    try:
        return pyodbc.connect(
            f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={db};UID={UID};PWD={PWD};"
            f"TrustServerCertificate=yes;Encrypt=no;",
            autocommit=autocommit
        )
    except Exception as e:
        print(f"[DB ERROR] {e}")
        sys.exit(1)

def get_tables(cur):
    cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    return [r[0] for r in cur.fetchall()]

def get_columns(cur, table):
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, table)
    return cur.fetchall()

def is_string(datatype:str)->bool:
    return datatype.lower() in ("nvarchar","varchar","text","nchar","char")

def looks_like_pii(col:str)->bool:
    col_l=col.lower()
    return any(k in col_l for k in PII_KEYWORDS)

# ────────────────────── AI PROMPT GENERATION ────────────────────── #

def load_cache()->dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE,"r",encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache:dict):
    with open(CACHE_FILE,"w",encoding="utf-8") as f:
        json.dump(cache,f,indent=2)

def hash_prompt(prompt:str)->str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()

def call_lmstudio(prompt:str)->str:
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE
    }
    try:
        r=requests.post(LM_URL,json=payload,timeout=60)
        r.raise_for_status()
        return r.json().get("choices",[{}])[0].get("text","").strip()
    except Exception as e:
        print(f"[LM ERROR] {e} :: {prompt[:60]}…")
        return ""

def async_generate(prompts:list, cache:dict)->dict:
    """Return dict {prompt_hash: generated_text}"""
    results={}
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        fut_to_hash={}
        for p in prompts:
            h=hash_prompt(p)
            if h not in cache:
                fut=pool.submit(call_lmstudio,p)
                fut_to_hash[fut]=h
        if len(fut_to_hash) > 0:
            for fut in tqdm(as_completed(fut_to_hash),total=len(fut_to_hash),
                            desc="⏳ Calling LM Studio"):
                h=fut_to_hash[fut]
                txt=fut.result()
                cache[h]=txt
        # else: no progress bar if no prompts
    results.update({h:cache[h] for h in (hash_prompt(p) for p in prompts)})
    return results

# ────────────────────── MAIN PIPELINE ────────────────────── #

def main():
    os.makedirs(OUTPUT_DIR,exist_ok=True)
    cache = load_cache()
    print(f"🔗 Connecting to source DB: {SOURCE_DB}")
    with connect(SOURCE_DB) as src_conn:
        src_cur = src_conn.cursor()

        print(f"🔗 Preparing destination DB: {DEST_DB}")
        with connect("master", autocommit=True) as master:
            master.cursor().execute(f"IF DB_ID('{DEST_DB}') IS NULL CREATE DATABASE [{DEST_DB}]")

        with connect(DEST_DB) as dest_conn:
            dest_cur = dest_conn.cursor()
            dest_cur.fast_executemany = True

            for table in get_tables(src_cur):
                print(f"\n=== TABLE: {table} ===")
                cols = get_columns(src_cur,table)
                colnames=[c for c,_ in cols]
                coltypes={c:t for c,t in cols}
                # identify columns needing AI
                ai_cols = [c for c,t in cols
                           if is_string(t) and c.lower() in FREE_TEXT_FIELDS]

                # ------- Phase 1: read rows & build prompts ------- #
                prompts=[]
                row_meta=[]  # list of (row_id, colname, prompt_hash)
                src_cur.execute(f"SELECT COUNT(*) FROM [{table}]")
                total_rows=src_cur.fetchone()[0]
                print(f"Rows: {total_rows} | AI columns: {ai_cols}")
                src_cur.execute(f"SELECT * FROM [{table}]")
                rid=0
                while True:
                    batch = src_cur.fetchmany(BATCH_SIZE)
                    if not batch: break
                    for row in batch:
                        rid+=1
                        row_dict = dict(zip(colnames,row))
                        for col in ai_cols:
                            title = row_dict.get("title") or row_dict.get("jobtitle") or ""
                            dept  = row_dict.get("department") or ""
                            prompt = (f"Generate realistic {col} text for an HRIS dataset. "
                                      f"Role title: '{title}'. Department: '{dept}'. "
                                      f"Use natural language, avoid any real personal data.")
                            prompts.append(prompt)
                            row_meta.append((rid,col,hash_prompt(prompt)))
                # ------- Phase 2: async AI calls (only new) ------- #
                if prompts:
                    _=async_generate(prompts,cache)
                    save_cache(cache)

                # ------- Phase 3: rewind & export with replacements ------- #
                src_cur.execute(f"SELECT * FROM [{table}]")
                # ensure dest table exists
                col_defs=[]
                for c,t in cols:
                    t_l=t.lower()
                    if t_l in ("nvarchar","varchar"):
                        col_defs.append(f"[{c}] {t_l}(max)")
                    elif t_l in ("nchar","char"):
                        col_defs.append(f"[{c}] {t_l}(32)")
                    else:
                        col_defs.append(f"[{c}] {t_l}")
                dest_cur.execute(f"IF OBJECT_ID('[{table}]','U') IS NULL "
                                 f"CREATE TABLE [{table}] ({', '.join(col_defs)})")
                dest_conn.commit()

                csv_path=os.path.join(OUTPUT_DIR,f"{table}.csv")
                with open(csv_path,"w",newline="",encoding="utf-8") as f_csv:
                    writer=csv.writer(f_csv)
                    writer.writerow(colnames)

                    buffer=[]
                    rid=0
                    src_cur.execute(f"SELECT * FROM [{table}]")
                    ai_lookup=defaultdict(dict)  # {row_id: {col: text}}
                    for (row_id,col,h) in row_meta:
                        ai_lookup[row_id][col]=cache[h]

                    FLUSH_EVERY = 1  # flush buffer after every row to avoid MemoryError
                    processed_rows = 0
                    with tqdm(total=total_rows, desc=f"Exporting {table}") as pbar:
                        while True:
                            batch=src_cur.fetchmany(BATCH_SIZE)
                            if not batch: break
                            for row in batch:
                                rid+=1
                                out=[]
                                for col,val in zip(colnames,row):
                                    col_l=col.lower()
                                    typ=coltypes[col]
                                    # 1) AI replacement if needed (only for string columns)
                                    if is_string(typ) and col in ai_cols:
                                        val = ai_lookup.get(rid,{}).get(col,
                                                fake.sentence(nb_words=12))
                                    # 2) Faker for simple PII (only for string columns)
                                    elif is_string(typ) and looks_like_pii(col):
                                        if "email" in col_l: val=fake.email()
                                        elif "name" in col_l: val=fake.name()
                                        elif "phone" in col_l: val=fake.phone_number()
                                        else: val=fake.word()
                                    # 3) For non-string columns, always use the original value
                                    out.append(val)
                                writer.writerow(out)
                                buffer.append(out)
                                processed_rows += 1
                                pbar.update(1)
                                # bulk insert buffer
                                if len(buffer) >= FLUSH_EVERY:
                                    insert_sql = (f"INSERT INTO [{table}] "
                                                  f"({', '.join('['+c+']' for c in colnames)}) "
                                                  f"VALUES ({', '.join('?'*len(colnames))})")
                                    dest_cur.executemany(insert_sql,buffer)
                                    dest_conn.commit()
                                    buffer.clear()
                    # flush remaining
                    if buffer:
                        insert_sql = (f"INSERT INTO [{table}] "
                                      f"({', '.join('['+c+']' for c in colnames)}) "
                                      f"VALUES ({', '.join('?'*len(colnames))})")
                        dest_cur.executemany(insert_sql,buffer)
                        dest_conn.commit()
                print(f"✔ Exported → {csv_path}")

    print("\n🏁  All tables processed and test data written.")

if __name__=="__main__":
    t0=time.time()
    main()
    print(f"\nElapsed: {time.time()-t0:,.1f}s")
