import http.client
import json
import pandas as pd
import pyodbc




# API Configuration
API_HOST = "jsearch.p.rapidapi.com"
API_KEY = "a48da2297emsh1db048c95a739d2p13f192jsn5a07b5afdde1"

# Establish HTTP Connection
conn = http.client.HTTPSConnection(API_HOST)

# API Headers
headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

print("API configuration is set")

conn.request("GET", "/search?query=Data%20Analyst%20jobs%20in%20New%20York&page=1&num_pages=1&country=us&date_posted=all", headers=headers)

# Get Response
res = conn.getresponse()
data = res.read().decode("utf-8")



# Convert Response to JSON
try:
    job_data = json.loads(data).get("data", [])
except json.JSONDecodeError:
    print("Error decoding JSON response")
    job_data = []

print("Data is retrieved and converted into JSON")

# Convert to Pandas DataFrame
df = pd.DataFrame(job_data, columns=["job_id", "job_title", "employer_name", "job_city", "job_description", "job_apply_link", "job_posted_at_datetime_utc"])

path = r"C:\Users\leven\OneDrive\Desktop\test_folder\JobAPIoutput.csv"
df.to_csv(path, index=False)



# Convert Response to JSON
try:
    job_data = json.loads(data).get("data", [])
except json.JSONDecodeError:
    print("Error decoding JSON response")
    job_data = []

# Convert to Pandas DataFrame
df = pd.DataFrame(job_data, columns=["job_id", "job_title", "employer_name", "job_city", "job_description", "job_apply_link", "job_posted_at_datetime_utc"])

path = r"C:\Users\leven\OneDrive\Desktop\test_folder\JobAPIoutput.csv"
df.to_csv(path, index=False)

print("data is saved into csv and inserted into pd dataframe")

server = 'LEVENT\\SQLEXPRESS'  # Double backslashes for escape sequences
database = 'Jobs'

# Correct connection string
conn = pyodbc.connect(
    f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
)
cursor = conn.cursor()

print("SQL Server connection is established")

# Convert Response to JSON
try:
    job_data = json.loads(data).get("data", [])
except json.JSONDecodeError:
    print("Error decoding JSON response")
    job_data = []

# Convert to Pandas DataFrame
df = pd.DataFrame(job_data, columns=["job_id", "job_title", "employer_name", "job_city", "job_description", "job_apply_link", "job_posted_at_datetime_utc"])

path = r"C:\Users\leven\OneDrive\Desktop\test_folder\JobAPIoutput.csv"
df.to_csv(path, index=False)

print("CSV output is created")

for index, row in df.iterrows():
    cursor.execute(
    """
        IF NOT EXISTS (SELECT 1 FROM JobPostings WHERE JobID = ?)
        INSERT INTO JobPostings (JobID, JobTitle, CompanyName, Location, JobDescription, JobURL, PostedDate)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, 
        (
        row["job_id"],
        row["job_id"],
        row["job_title"],
        row["employer_name"],
        row["job_city"],
        row["job_description"],
        row["job_apply_link"],
        row["job_posted_at_datetime_utc"]
        )
                    )

# Commit and Close Connection
conn.commit()

print("df is ingested into the database table")

