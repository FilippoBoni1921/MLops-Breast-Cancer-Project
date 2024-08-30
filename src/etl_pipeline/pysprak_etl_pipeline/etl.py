from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.types import BinaryType
import cv2
import numpy as np
from google.cloud import storage
from google.oauth2 import service_account

from src.constants import KEY_FILE_PATH, BUCKET_RAW_DATA, BUCKET_PREPROC, IMAGE_LOADING_PATH

def initialize_spark():
    """Initialize and return Spark session."""
    return SparkSession.builder \
        .appName('pyspark-run-with-local-folder') \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

def configure_gcs(spark, key_path):
    """Configure GCS settings for Spark session."""
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", key_path)
    spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.gs.project.id", "filippo-boni-project")

def load_image_data(spark, loading_path):
    """Load image data from a GCS bucket."""
    loading_path = "gs://" + loading_path +"*"
    return spark.read.format("binaryFile") \
        .load(loading_path)

def resize_image(image_data, width=100, height=100):
    """Resize image using OpenCV."""
    nparr = np.frombuffer(image_data, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is not None:
        resized_img = cv2.resize(img, (width, height))
        _, img_encoded = cv2.imencode('.png', resized_img)
        return img_encoded.tobytes()
    return None

def resize_image_udf(width=100, height=100):
    """Define and return the UDF for resizing images."""
    return udf(lambda data: resize_image(data, width, height), BinaryType())

def process_partition(iterator, credentials_broadcast, bucket_raw, bucket_preproc):
    """Process each partition and upload resized images to GCS."""
    client = storage.Client(credentials=credentials_broadcast.value)
    bucket = client.bucket(bucket_raw)
    
    for row in iterator:
        image_data = row['resized_content']
        subpath = row['subpath']
        
        if image_data is not None:
            blob = bucket.blob(f"{bucket_preproc}/{subpath}")
            if isinstance(image_data, bytearray):
                image_data = bytes(image_data)
            blob.upload_from_string(image_data, content_type='image/png')

def pyspark_preprocess(key_path, bucket_raw, bucket_preproc, loading_path):

    spark = initialize_spark()
    configure_gcs(spark, key_path)
    
    image_df = load_image_data(spark, loading_path)
    
    sc = spark.sparkContext
    credentials = service_account.Credentials.from_service_account_file(key_path)
    credentials_broadcast = sc.broadcast(credentials)
    
    prefix = "gs://breast-cancer-images-raw/raw_breast_images/"
    image_df = image_df.withColumn(
        "subpath", 
        expr(f"substring(path, length('{prefix}') + 1, length(path))")
    )
    
    resized_image_df = image_df.withColumn("resized_content", resize_image_udf()("content"))
    
    num_partitions = sc.defaultParallelism * 4
    resized_image_df = resized_image_df.repartition(num_partitions)
    
    resized_image_df.rdd.foreachPartition(lambda iterator: process_partition(iterator, credentials_broadcast,bucket_raw, bucket_preproc))
    print("Partitions processed successfully.")

    spark.stop()

if __name__ == "__main__":
    pyspark_preprocess(KEY_FILE_PATH, BUCKET_RAW_DATA, BUCKET_PREPROC, IMAGE_LOADING_PATH)
