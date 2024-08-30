from google.cloud import storage
from google.oauth2 import service_account
import os

# Path to your service account key file
key_path = '/Users/filippoboni/Desktop/MLOPS/move_images_to_gcp/filippo-boni-project-f61113b1f868.json'

# Create credentials object
credentials = service_account.Credentials.from_service_account_file(key_path)

# Initialize the GCS client with the credentials
storage_client = storage.Client(credentials=credentials)

# Specify your bucket name and GCS folder
bucket_name = 'breast-cancer-images-raw'
bucket = storage_client.bucket(bucket_name)

# Define local directory and GCS path
local_folder = 'data'
gcs_folder = 'raw_breast_images/'

# Ensure the local base folder exists
if not os.path.exists(local_folder):
    os.makedirs(local_folder)

# List and download files
blobs = bucket.list_blobs(prefix=gcs_folder)

for blob in blobs:
    if blob.name.endswith(".png"):
        # Define the local file path with the same structure as the GCS bucket
        # Remove the GCS prefix from the blob name to get the relative path
        relative_path = os.path.relpath(blob.name, gcs_folder)
        local_file_path = os.path.join(local_folder, relative_path)
        
        # Create the local directory if it doesn't exist
        local_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        # Download the file
        blob.download_to_filename(local_file_path)
        print(f"Downloaded {blob.name} to {local_file_path}")

print("All files downloaded successfully!")
