import streamlit as st
import pandas as pd
import spacy
import re
import json
import docx2txt
import PyPDF2
import tempfile
import os
import pymysql
import boto3
import snowflake.connector
from io import StringIO

# ---------------- CONFIG ----------------
nlp = spacy.load("en_core_web_sm")

# ---------- MYSQL ----------
MYSQL_HOST = "localhost"
MYSQL_USER = "resume_user"
MYSQL_PASSWORD = "resume123"
MYSQL_DB = "resume_db"

# ---------- AWS S3 ----------
AWS_ACCESS_KEY = "AKIAQW3CPVFNF72AGIUC"
AWS_SECRET_KEY = "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"
AWS_REGION = "ap-south-1"
S3_BUCKET = "mysql-s3-csv"
S3_FILE = "mysql_combined_table.csv"

# ---------- SNOWFLAKE ----------
SNOWFLAKE_USER = "atharva"
SNOWFLAKE_PASSWORD = "Atharvakarade@123"
SNOWFLAKE_ACCOUNT = "huzunya-pv30101"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "RESUME_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"

OUTPUT_FOLDER = "csv_output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

st.set_page_config(page_title="Resume NER Pipeline", layout="wide")

# ---------------- DB SETUP ----------------
def setup_mysql():
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cur.execute(f"USE {MYSQL_DB}")

    cur.execute("CREATE TABLE IF NOT EXISTS combined (name VARCHAR(255), email VARCHAR(255), phone VARCHAR(50), education VARCHAR(255), skills VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS names (name VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS emails (email VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS phones (phone VARCHAR(50))")
    cur.execute("CREATE TABLE IF NOT EXISTS education (education VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS skills (skills VARCHAR(255))")

    conn.commit()
    conn.close()

def insert_mysql(df):
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    cur = conn.cursor()

    for _, r in df.iterrows():
        cur.execute("INSERT INTO names VALUES (%s)", (r["Name"],))
        cur.execute("INSERT INTO emails VALUES (%s)", (r["Email"],))
        cur.execute("INSERT INTO phones VALUES (%s)", (r["Phone"],))
        cur.execute("INSERT INTO education VALUES (%s)", (r["Education"],))
        cur.execute("INSERT INTO skills VALUES (%s)", (r["Skills"],))
        cur.execute("INSERT INTO combined VALUES (%s,%s,%s,%s,%s)",
                    (r["Name"], r["Email"], r["Phone"], r["Education"], r["Skills"]))

    conn.commit()
    conn.close()

# ---------------- S3 UPLOAD ----------------
def upload_to_s3():
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB)
    df = pd.read_sql("SELECT * FROM combined", conn)
    conn.close()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.client("s3",
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGION)

    s3.put_object(Bucket=S3_BUCKET, Key=S3_FILE, Body=csv_buffer.getvalue())

# ---------------- SNOWFLAKE LOAD ----------------
def load_snowflake():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()

    cur.execute("""
        COPY INTO RESUME_DATA
        FROM @resume_stage/mysql_combined_table.csv
        FILE_FORMAT = (FORMAT_NAME = my_csv_format)
        ON_ERROR = 'CONTINUE'
    """)

    conn.commit()
    conn.close()

# ---------------- EXTRACTORS ----------------
def extract_email(text):
    m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return m.group(0) if m else "NA"

def extract_phone(text):
    m = re.search(r"\b(?:\+?\d{1,3}[\s-]?)?\d{10}\b", text)
    return m.group(0) if m else "NA"

def extract_name(text):
    lines = text.splitlines()
    lines = [l.strip() for l in lines if l.strip()][:100]

    BAD_WORDS = {
        "experience", "summary", "profile", "developer", "engineer",
        "skills", "project", "resume", "education", "objective",
        "data", "analyst", "analysis", "curriculum", "intern", "manager",
        "company", "role"
    }

    for line in lines:
        if line.lower().startswith(("name:", "name -", "name ")):
            name = line.split(":", 1)[-1].replace("-", "").strip()
            if 1 < len(name.split()) <= 6:
                return name

    header_text = " ".join(lines[:60])
    doc = nlp(header_text)
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            name = ent.text.strip()
            if 1 < len(name.split()) <= 6 and all(w.lower() not in BAD_WORDS for w in name.split()):
                return name

    for line in lines[:40]:
        if re.search(r"[0-9@#/$%&*<>]", line):
            continue
        parts = line.split()
        if 1 < len(parts) <= 6 and all(w.lower() not in BAD_WORDS for w in parts):
            return line.strip()

    return "NA"

def extract_education(text):
    words = ["B.Tech","B.E","M.Tech","MBA","B.Sc","M.Sc","PhD"]
    return ", ".join([w for w in words if w.lower() in text.lower()]) or "NA"

def extract_skills(text):
    tech = ["Python","Java","SQL","Machine Learning","Excel","Power BI","C++","JavaScript"]
    return ", ".join(set([t for t in tech if t.lower() in text.lower()])) or "NA"

# ---------------- FILE READERS ----------------
def read_pdf(file):
    reader = PyPDF2.PdfReader(file)
    return "\n".join([p.extract_text() or "" for p in reader.pages])

def read_docx(file):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(file.read())
        return docx2txt.process(tmp.name)

def read_txt(file):
    return file.read().decode("utf-8", errors="ignore")

def read_json(file):
    return json.dumps(json.load(file))

# ---------------- UI ----------------
st.title("Resume NER → MySQL → S3 → Snowflake")

files = st.file_uploader("Upload resumes", type=["pdf","docx","txt","json"], accept_multiple_files=True)

if st.button("Run Pipeline"):
    if not files:
        st.warning("Upload files first")
    else:
        data = []

        for f in files:
            ext = f.name.split(".")[-1].lower()
            text = ""

            if ext == "pdf": text = read_pdf(f)
            elif ext == "docx": text = read_docx(f)
            elif ext == "txt": text = read_txt(f)
            elif ext == "json": text = read_json(f)

            data.append({
                "Name": extract_name(text),
                "Email": extract_email(text),
                "Phone": extract_phone(text),
                "Education": extract_education(text),
                "Skills": extract_skills(text)
            })

        df = pd.DataFrame(data)

        # Save CSVs
        df[["Name"]].to_csv("csv_output/names.csv", index=False)
        df[["Email"]].to_csv("csv_output/emails.csv", index=False)
        df[["Phone"]].to_csv("csv_output/phones.csv", index=False)
        df[["Education"]].to_csv("csv_output/education.csv", index=False)
        df[["Skills"]].to_csv("csv_output/skills.csv", index=False)
        df.to_csv("csv_output/combined.csv", index=False)

        setup_mysql()
        insert_mysql(df)
        upload_to_s3()
        load_snowflake()

        st.success("Pipeline completed!")
        st.dataframe(df)
