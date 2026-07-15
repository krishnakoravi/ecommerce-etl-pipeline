"""
Provisioning helper for the AWS resources this pipeline uses: S3 buckets (raw +
processed zones) and a Glue crawler over the processed zone.

This is intentionally boto3-scripted rather than Terraform/CloudFormation, since
Terraform isn't part of the target tech stack here -- but it makes the repo
self-contained: a reviewer with AWS credentials configured can run this once to
stand up the infrastructure described in the README's architecture diagram.

Usage:
    python setup_aws.py --bucket-name your-unique-bucket-name --region us-east-1
"""
import argparse

import boto3
from botocore.exceptions import ClientError


def create_bucket(s3_client, bucket_name, region):
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"Created bucket: {bucket_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"Bucket already exists: {bucket_name}")
        else:
            raise


def create_prefixes(s3_client, bucket_name):
    """S3 has no real directories, but writing zero-byte objects with trailing
    slashes makes the zones visible in the console for a reviewer poking around."""
    for prefix in ["raw/", "processed/", "scripts/"]:
        s3_client.put_object(Bucket=bucket_name, Key=prefix)
    print("Created raw/, processed/, scripts/ prefixes")


def create_glue_crawler(glue_client, bucket_name, crawler_name, iam_role_arn):
    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=iam_role_arn,
            DatabaseName="ecommerce_clickstream",
            Targets={"S3Targets": [{"Path": f"s3://{bucket_name}/processed/"}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
        )
        print(f"Created Glue crawler: {crawler_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"Glue crawler already exists: {crawler_name}")
        else:
            raise


def main(bucket_name, region, iam_role_arn):
    s3_client = boto3.client("s3", region_name=region)
    create_bucket(s3_client, bucket_name, region)
    create_prefixes(s3_client, bucket_name)

    if iam_role_arn:
        glue_client = boto3.client("glue", region_name=region)
        create_glue_crawler(glue_client, bucket_name, "ecommerce-clickstream-crawler", iam_role_arn)
    else:
        print("No --iam-role-arn provided; skipping Glue crawler creation.")

    print("\nDone. Next steps:")
    print(f"  aws s3 cp data/sample/clickstream_events.json s3://{bucket_name}/raw/")
    print(f"  aws s3 cp run_pipeline.py s3://{bucket_name}/scripts/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--iam-role-arn", default=None, help="Glue service role ARN")
    args = parser.parse_args()

    main(args.bucket_name, args.region, args.iam_role_arn)
