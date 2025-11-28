import json
import boto3
import xml.etree.ElementTree as ET
import csv
import io
import datetime
import os

# Initialize AWS service clients
s3 = boto3.client("s3")
glue = boto3.client("glue")
cw = boto3.client("cloudwatch")

# Name of the Glue job for data processing
GLUE_JOB_NAME = "Customer-360"

# Function to publish metrics to CloudWatch
def put_cw_metric(namespace, name, value, unit="Count"):
    try:
        cw.put_metric_data(
            Namespace=namespace,
            MetricData=[{"MetricName": name, "Value": value, "Unit": unit}]
        )
    except Exception as e:
        print("Failed to publish CW metric:", e)

# Report a validation failure metric
def report_validation_failure():
    put_cw_metric("Customer360", "LambdaValidationFailures", 1)

# Validate if a date string is in the correct format (YYYY-MM-DD)
def is_valid_date(date_str):
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except:
        return False

# Process XML records to extract valid and invalid customer data
def process_xml_records(content):
    root = ET.fromstring(content)  # Parse the XML content
    valid_customers = []
    invalid_customers = []

    # Loop through each Customer element in the XML
    for cust in root.findall(".//Customer"):
        cid = cust.find("CustomerID")
        name = cust.find("Name")
        city = cust.find("City")
        # Check for missing or invalid fields
        if cid is None or name is None or city is None:
            invalid_customers.append(cust)
            continue
        if not cid.text or not cid.text.strip().isdigit():
            invalid_customers.append(cust)
            continue
        if not name.text or name.text.strip() == "":
            invalid_customers.append(cust)
            continue
        if not city.text or city.text.strip() == "":
            invalid_customers.append(cust)
            continue
        valid_customers.append(cust)  

    return valid_customers, invalid_customers

# Build XML from a list of customer data
def build_xml(customers):
    root = ET.Element("Customers")
    for cust in customers:
        root.append(cust) 
    return ET.tostring(root, encoding="utf-8")  

# Process JSON records to validate and separate valid and invalid records
def process_json_records(content):
    records = json.loads(content)  
    valid = []
    invalid = []

    # Loop through each record in the JSON
    for rec in records:
        try:
            # Validate required fields and their values
            if ("CustomerID" not in rec or not str(rec["CustomerID"]).isdigit() or
                "Amount" not in rec or float(rec["Amount"]) <= 0 or
                "Product" not in rec or rec["Product"].strip() == "" or
                "Date" not in rec or not is_valid_date(rec["Date"])):
                invalid.append(rec)
            else:
                valid.append(rec)
        except:
            invalid.append(rec)  

    return valid, invalid

# Process CSV records to validate and separate valid and invalid rows
def process_csv_records(content):
    f = io.StringIO(content)  
    reader = csv.DictReader(f) 
    valid = []
    invalid = []

    # Loop through each row in the CSV
    for row in reader:
        try:
            # Validate required fields and their values
            if (not row["CustomerID"].isdigit() or
                int(row["Rating"]) < 1 or int(row["Rating"]) > 5 or
                row["Feedback"].strip() == ""):
                invalid.append(row)
            else:
                valid.append(row)
        except:
            invalid.append(row)  

# Main handler function for AWS Lambda
def lambda_handler(event, context):
    try:
        record = event["Records"][0]  # Get the first record from the event
    except Exception as e:
        print("ERROR: 'Records' missing or invalid event:", e)
        report_validation_failure()
        return {"status": "No Records"}

    bucket = record["s3"]["bucket"]["name"]  # Get the S3 bucket name
    key = record["s3"]["object"]["key"]  # Get the S3 object key
    print(f"Processing: {key}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)  
        content = obj["Body"].read().decode("utf-8")  
    except Exception as e:
        print("Failed to read object:", e)
        report_validation_failure()
        return {"status": "Failed to read file"}

    total_valid = 0
    total_invalid = 0

    # Process the file based on its type (XML, JSON, CSV)
    if key.endswith(".xml"):
        valid, invalid = process_xml_records(content)  # Process XML content
        if valid:
            validated_xml = build_xml(valid)
            s3.put_object(Bucket=bucket, Key=key.replace("raw/", "validated/"), Body=validated_xml)  # Save valid customers
            total_valid += len(valid)
        if invalid:
            error_xml = build_xml(invalid)
            s3.put_object(Bucket=bucket, Key=key.replace("raw/", "error/"), Body=error_xml)  # Save invalid customers
            total_invalid += len(invalid)

    elif key.endswith(".json"):
        valid, invalid = process_json_records(content)  # Process JSON content
        if valid:
            s3.put_object(Bucket=bucket, Key=key.replace("raw/", "validated/"), Body=json.dumps(valid, indent=2))  # Save valid records
            total_valid += len(valid)
        if invalid:
            s3.put_object(Bucket=bucket, Key=key.replace("raw/", "error/"), Body=json.dumps(invalid, indent=2))  # Save invalid records
            total_invalid += len(invalid)

    elif key.endswith(".csv"):
        valid, invalid = process_csv_records(content)  # Process CSV content
        if valid:
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=valid[0].keys())  # Prepare to write CSV
            writer.writeheader()
            writer.writerows(valid)
            s3.put_object(Bucket=bucket, Key=key.replace("raw/", "validated/"), Body=output.getvalue())  # Save valid rows
            total_valid += len(valid)
        if invalid:
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=invalid[0].keys())
            writer.writeheader()
            writer.writerows(invalid)
            s3.put_object(Bucket=bucket, Key=key.replace("raw/", "error/"), Body=output.getvalue())  # Save invalid rows
            total_invalid += len(invalid)

    # Check for previously validated objects and update totals
    validated_objects = s3.list_objects_v2(Bucket=bucket, Prefix="validated/")
    for obj in validated_objects.get('Contents', []):
        file_key = obj["Key"]
        obj2 = s3.get_object(Bucket=bucket, Key=file_key)
        content2 = obj2["Body"].read().decode("utf-8")

        if file_key.endswith(".xml"):
            valid2, _ = process_xml_records(content2)
        elif file_key.endswith(".json"):
            valid2, _ = process_json_records(content2)
        elif file_key.endswith(".csv"):
            valid2, _ = process_csv_records(content2)
        else:
            continue

        total_valid += len(valid2)

    # Check for previously error objects and update invalid totals
    error_objects = s3.list_objects_v2(Bucket=bucket, Prefix="error/")
    for obj in error_objects.get('Contents', []):
        file_key = obj["Key"]
        obj2 = s3.get_object(Bucket=bucket, Key=file_key)
        content2 = obj2["Body"].read().decode("utf-8")

        if file_key.endswith(".xml"):
            _, invalid2 = process_xml_records(content2)
        elif file_key.endswith(".json"):
            _, invalid2 = process_json_records(content2)
        elif file_key.endswith(".csv"):
            _, invalid2 = process_csv_records(content2)
        else:
            continue

        total_invalid += len(invalid2)

    # Calculate the Data Quality Score
    dq_score = (
        (total_valid / (total_valid + total_invalid)) * 100
        if (total_valid + total_invalid) > 0
        else 0
    )

    if total_invalid > 0:
        report_validation_failure()

    # Trigger the Glue job if there are valid records
    if total_valid > 0:
        glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--JOB_NAME": "Customer-360",
                "--VALIDATED_XML_PATH": "s3://customer-360-datalake/validated/customer_details/",
                "--VALIDATED_JSON_PATH": "s3://customer-360-datalake/validated/customer_purchases/",
                "--VALIDATED_CSV_PATH": "s3://customer-360-datalake/validated/customer_feedback/",
                "--DYNAMO_TABLE": "customer360_etl_tracking",
                "--RDS_JDBC_URL": "jdbc:postgresql://customer360-db.cw3aw0iuuvtz.us-east-1.rds.amazonaws.com:5432/postgres",
                "--RDS_USER": "postgres",
                "--RDS_PASSWORD": "customer360!",
                "--DQ_SCORE": str(dq_score),
                "--ERROR_COUNT": str(total_invalid)
            }
        )

    return {"status": "Record-level processing complete"}