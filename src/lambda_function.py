import csv
import json
import boto3

def lambda_handler(event, context):
    """
    Lambda function that processes a CSV file from S3, 
    calculates salary totals by country, and saves results.
    """
    # Get S3 bucket and file information from the event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    
    # Log file information
    print(f"Processing file {key} from bucket {bucket}")
    
    # Get the CSV file from S3
    s3 = boto3.resource('s3')
    csv_file = s3.Object(bucket, key).get()['Body'].read().decode('utf-8').splitlines()
    
    # Parse CSV
    reader = csv.reader(csv_file)
    headers = next(reader)  # Skip header row
    
    # Process data by country
    india_salaries = []
    us_salaries = []
    
    for row in reader:
        if row[3] == 'India':
            india_salaries.append(int(row[2]))
        else:
            us_salaries.append(int(row[2]))
    
    # Calculate totals
    india_total = sum(india_salaries)
    us_total = sum(us_salaries)
    
    print(f"India total: {india_total}, US total: {us_total}")
    
    # Save results back to S3
    if key == 'input.csv':
        result = f"total india,us salary spend is ,{india_total},{us_total}"
        s3 = boto3.client('s3')
        s3.put_object(Body=result, Bucket=bucket, Key='output.txt')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }