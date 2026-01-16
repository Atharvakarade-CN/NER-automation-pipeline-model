 NER-automation-pipeline-model — HEAVY DOCUMENTATION (MASTER VERSION)
Streamlit UI → MySQL → AWS S3 → Snowflake → Power BI
1. Project Overview (High-Level Explanation)
This project is a fully automated, production-grade Resume Entity Recognition (NER) and Data Engineering 
Pipeline.
Its objective is to allow a user (or client) to:
- Upload Files through a Streamlit UI
- Automatically Extract Entities like: Name, Email, Phone, Skills, Education
- Automatically store those extracted entities into: MySQL, AWS S3, Snowflake, - Automatically refresh 
dashboards in Power BI
No manual steps are required. Once the file is uploaded → the entire pipeline runs end-to-end.
Skills demonstrated:
- (NER)
- Cloud computing (AWS, Snowflake)
- ETL pipeline development
- Database engineering
- Automation with schedulers
- Full-stack development (Streamlit)
- Business intelligence (Power BI)
2. Complete Architecture Diagram (Textual Form)
CLIENT MACHINE (Browser / IP URL)
 │
STREAMLIT UI (Local)
- Upload PDF/DOCX/CSV/JSON
- Extract Name/Email/Phone
- Extract Skills/Education
 │ │
 ▼ ▼
MySQL Local AWS S3 (Bucket)
candidate_details extracted_data.csv
 │ │
 ▼ ▼
Snowflake Cloud CANDIDATE_DETAILS 
 │ │
 ▼ ▼
Snowflake Warehouse
(BI Consumption Layer) resume_data, furniture, blood_report_data, etc.
 │ │
 ▼ ▼
POWER BI DASHBOARD
Auto Refresh 5 sec via Page Refresh Interval
3. Detailed Component Explanation
3.1 Streamlit UI
- Accepts multiple resume file uploads
- Extracts raw text from PDF, DOCX, JSON, CSV, TXT
- Runs NER extraction: extract_name(), extract_email(), extract_phone(), extract_skills(),extract_education() -
Displays extracted data in table
- Allows CSV download
- Stores automatically in MySQL, Snowflake, AWS S3
3.2 NER Extraction System
- Regex-based, fast, light-weight
- Fields: Name (first 5 lines, uppercase, NAME: <value>), Email, Phone (+91, spaced,hyphenated), Skills 
(keyword-based), Education (degree pattern)
4. MySQL Auto Storage Layer
- Table: candidate_details
- Logic: INSERT ... ON DUPLICATE KEY UPDATE (no duplicates, auto-updates)
5. AWS S3 Storage Layer
- Upload extracted CSV: extracted_data_<timestamp>.csv
- Acts as cloud backup, ETL ingestion zone, Snowflake input
6. Snowflake Cloud Warehouse Layer
- Table: CANDIDATE_INFO.PUBLIC.CANDIDATE_DETAILS
- MERGE INTO ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT
7. Power BI Real-Time Reporting
- Connects to Snowflake & PostgreSQL
- Page refresh every 5 sec for real-time updates
8. End-to-End Execution Flow
Step 1: User uploads file (resume.pdf, profile.docx, data.csv)
Step 2: Streamlit extracts entities
Step 3: Store in MySQL
Step 4: Upload CSV to S3
Step 5: Snowflake MERGE upserts records
Step 6: Power BI dashboard auto-refreshes
9. System Advantages
- No manual SQL
- No manual refresh
- Fully automated
- Highly scalable
- Cloud-enabled
- Multi-database & multi-format suppor
<img src="Screenshot 2026-01-16 183312.png" alt="Screenshot">
<img src="Screenshot_2026-01-16_183329.png" alt="Screenshot">
<img src="Screenshot_2026-01-16_183353.png" alt="Screenshot">

