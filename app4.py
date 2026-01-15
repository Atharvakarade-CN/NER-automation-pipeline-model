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
from io import StringIO

# ---------------- CONFIG ----------------
nlp = spacy.load("en_core_web_sm")

# ---------- MYSQL CONFIG ----------
MYSQL_HOST = "localhost"
MYSQL_USER = "resume_user"
MYSQL_PASSWORD = "resume123"
MYSQL_DB = "resume_db"

# ---------- AWS S3 CONFIG ----------
AWS_ACCESS_KEY = "AKIAQW3CPVFNF72AGIUC"
AWS_SECRET_KEY = "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"
AWS_REGION = "ap-southeast-2"
S3_BUCKET_NAME = "mysql-s3-csv"
S3_FILE_NAME = "mysql_combined_table.csv"

# ---------- FOLDERS ----------
OUTPUT_FOLDER = "csv_output"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

st.set_page_config(page_title="Resume NER System", layout="wide")

# ---------------- DB SETUP ----------------
def setup_database():
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cur.execute(f"USE {MYSQL_DB}")

    cur.execute("CREATE TABLE IF NOT EXISTS names (name VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS emails (email VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS phones (phone VARCHAR(50))")
    cur.execute("CREATE TABLE IF NOT EXISTS education (education VARCHAR(255))")
    cur.execute("CREATE TABLE IF NOT EXISTS skills (skills VARCHAR(255))")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS combined (
            name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            education VARCHAR(255),
            skills VARCHAR(255)
        )
    """)

    conn.commit()
    conn.close()

# ---------------- MYSQL INSERT ----------------
def insert_to_mysql(df):
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cur = conn.cursor()

    # ✅ APPENDS DATA (does not delete old data)

    for val in df["Name"]:
        cur.execute("INSERT INTO names (name) VALUES (%s)", (val,))
    for val in df["Email"]:
        cur.execute("INSERT INTO emails (email) VALUES (%s)", (val,))
    for val in df["Phone"]:
        cur.execute("INSERT INTO phones (phone) VALUES (%s)", (val,))
    for val in df["Education"]:
        cur.execute("INSERT INTO education (education) VALUES (%s)", (val,))
    for val in df["Skills"]:
        cur.execute("INSERT INTO skills (skills) VALUES (%s)", (val,))

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO combined (name, email, phone, education, skills)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row["Name"],
            row["Email"],
            row["Phone"],
            row["Education"],
            row["Skills"]
        ))

    conn.commit()
    conn.close()

# ---------------- S3 UPLOAD ----------------
def upload_mysql_to_s3():
    # Read table
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    df = pd.read_sql("SELECT * FROM combined", conn)
    conn.close()

    # Convert dataframe to csv in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_FILE_NAME,
        Body=csv_buffer.getvalue()
    )

# ---------------- ENTITY EXTRACTORS ----------------
def extract_email(text):
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else "NA"

def extract_phone(text):
    match = re.search(r"\b(?:\+?\d{1,3}[\s-]?)?\d{10}\b", text)
    return match.group(0) if match else "NA"

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
    edu_keywords = ["B.Tech", "B.E", "M.Tech", "MBA", "B.Sc", "M.Sc", "PhD"]
    found = [e for e in edu_keywords if e.lower() in text.lower()]
    return ", ".join(found) if found else "NA"

def extract_skills(text):
    skills = ["Python","Java","SQL","Machine Learning","Excel","Power BI","C++","JavaScript"]
    found = [s for s in skills if s.lower() in text.lower()]
    return ", ".join(set(found)) if found else "NA"

# ---------------- FILE READERS ----------------
def read_pdf(file):
    reader = PyPDF2.PdfReader(file)
    return "\n".join([page.extract_text() or "" for page in reader.pages])

def read_docx(file):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(file.read())
        return docx2txt.process(tmp.name)

def read_txt(file):
    return file.read().decode("utf-8", errors="ignore")

def read_json(file):
    data = json.load(file)
    return json.dumps(data)

# ---------------- UI ----------------
st.title("📄 Resume NER System with S3 Sync")

uploaded_files = st.file_uploader(
    "Upload resumes (PDF, DOCX, TXT, JSON)",
    type=["pdf","docx","txt","json"],
    accept_multiple_files=True
)

if st.button("🚀 Extract & Save"):
    if not uploaded_files:
        st.warning("Please upload files")
    else:
        all_data = []

        for file in uploaded_files:
            ext = file.name.split(".")[-1].lower()

            if ext == "pdf":
                text = read_pdf(file)
            elif ext == "docx":
                text = read_docx(file)
            elif ext == "txt":
                text = read_txt(file)
            elif ext == "json":
                text = read_json(file)
            else:
                continue

            all_data.append({
                "Name": extract_name(text),
                "Email": extract_email(text),
                "Phone": extract_phone(text),
                "Education": extract_education(text),
                "Skills": extract_skills(text)
            })

        df = pd.DataFrame(all_data)

        # Save CSVs
        df[["Name"]].to_csv(f"{OUTPUT_FOLDER}/names.csv", index=False)
        df[["Email"]].to_csv(f"{OUTPUT_FOLDER}/emails.csv", index=False)
        df[["Phone"]].to_csv(f"{OUTPUT_FOLDER}/phones.csv", index=False)
        df[["Education"]].to_csv(f"{OUTPUT_FOLDER}/education.csv", index=False)
        df[["Skills"]].to_csv(f"{OUTPUT_FOLDER}/skills.csv", index=False)
        df.to_csv(f"{OUTPUT_FOLDER}/combined.csv", index=False)

        # MySQL setup + insert
        setup_database()
        insert_to_mysql(df)

        # ✅ Upload MySQL table to S3
        upload_mysql_to_s3()

        st.success("✅ CSV saved + MySQL updated + S3 upload done!")
        st.dataframe(df)
