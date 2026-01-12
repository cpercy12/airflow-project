import boto3
import os

s3 = boto3.client(
    "s3",
    endpoint_url = "http://localhost:9000",
    aws_access_key_id = "minioadmin",
    aws_secret_access_key = "minioadmin",
)

local_raw_dir = "data/staging/"

files_to_upload = {
    "Sales_structured.csv": "sales/SalesOrderHeader.csv",
    "Customers_structured.csv": "customers/Customer.csv",
    "Product_structured.csv": "products/Product.csv",
    "SalesOrderDetail_structured.csv": "Sales/SalesOrderDetail.csv"

}

bucket_name = "raw"

for local_file, s3_object in files_to_upload.items():
    s3.upload_file(os.path.join(local_raw_dir, local_file), bucket_name, s3_object)
    print(f"{local_file} uploadé vers {s3_object}")

