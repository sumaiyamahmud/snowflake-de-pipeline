import boto3                          # AWS SDK — lets Python talk to AWS services
import pandas as pd                   # data manipulation library
from sqlalchemy import create_engine  # creates database connection
import os                             # lets us work with file paths

# Load secrets from .env file in the project root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# --- RDS Connection Details (loaded from .env) ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# --- S3 Config ---
S3_BUCKET = "de-portfolio-sumaiya"    # your S3 bucket name
S3_RAW_KEY = "raw/online_retail.csv"  # path inside the bucket — lands in raw/ folder

def extract_from_rds():
    print("Connecting to RDS...")
    
    # format: postgresql://username:password@host:port/database
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    print("Extracting data...")
    # read_sql runs a SQL query and returns results as a pandas dataframe

    df = pd.read_sql("SELECT * FROM public.online_retail", engine)

    
    print(f"Extracted {len(df)} rows")  # print how many rows we got
    return df                           # return the dataframe to use in the next step

def upload_to_s3(df):
    print("Saving to CSV...")
    
    # /tmp is a temporary folder in Linux — safe place to store files briefly
    local_path = "/tmp/online_retail.csv"
    
    # save the dataframe as a CSV file locally first
    # index=False means don't write the row numbers (0,1,2...) as a column
    df.to_csv(local_path, index=False)
    
    print("Uploading to S3...")
    # create an S3 client — this is how boto3 connects to S3
    s3 = boto3.client("s3")
    
    # upload_file takes 3 arguments:
    # 1. local file path — where the file is on your machine
    # 2. bucket name — which S3 bucket to upload to
    # 3. S3 key — what to call the file in S3 (including folder path)
    s3.upload_file(local_path, S3_BUCKET, S3_RAW_KEY)
    
    print(f"Uploaded to s3://{S3_BUCKET}/{S3_RAW_KEY}")

# this means: only run the code below if this script is run directly
# (not if it's imported by another script)
if __name__ == "__main__":
    df = extract_from_rds()   # step 1: extract from RDS
    upload_to_s3(df)          # step 2: upload to S3
    print("Done!")