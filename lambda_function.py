import boto3
import csv
import json
import os

s3_client = boto3.client('s3')

INPUT_BUCKET = "josiaschweizer-input-bucket"
OUTPUT_BUCKET = "josiaschweizer-output-bucket"

def convert_csv_to_json(input_bucket, output_bucket, input_key):
    try:
        local_csv_path = "/tmp/input.csv"
        s3_client.download_file(input_bucket, input_key, local_csv_path)
        
        json_data = []
        with open(local_csv_path, mode="r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                json_data.append(row)
        
        local_json_path = "/tmp/output.json"
        with open(local_json_path, "w", encoding="utf-8") as json_file:
            json.dump(json_data, json_file, indent=4)
        
        output_key = input_key.replace(".csv", ".json")
        
        s3_client.upload_file(local_json_path, output_bucket, output_key)
        
        print(f"Successfully processed {input_key} and saved as {output_key} in {output_bucket}")
    except Exception as e:
        print(f"Error processing file {input_key}: {e}")

def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            input_key = record["s3"]["object"]["key"]
            print(f"Processing file: {input_key}")
            convert_csv_to_json(INPUT_BUCKET, OUTPUT_BUCKET, input_key)
    except Exception as e:
        print(f"Error in Lambda function: {e}")
