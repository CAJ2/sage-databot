from prefect import flow
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket


@flow
def setup_db_export():
    """
    Setup the database export flow.
    """
    # Load the database connection
    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Load the AWS credentials
    aws = AwsCredentials.load("do-spaces")
    bucket = Variable.get("spaces_db_bucket")
    if not bucket:
        raise ValueError(
            "No bucket name provided. Please set the 'spaces_db_bucket' variable."
        )
    endpoint = "https://fra1.digitaloceanspaces.com"
    region = "fra1"
    q_str = f"AWS_ACCESS_KEY_ID={aws.aws_access_key_id}&AWS_SECRET_ACCESS_KEY={aws.aws_secret_access_key.get_secret_value()}&AWS_REGION={region}&AWS_ENDPOINT={endpoint}"

    # Create the S3 bucket client
    s3 = S3Bucket(
        bucket_name=bucket,
        credentials=aws,
    )

    schedules = crdb.fetch_all("SHOW SCHEDULES")
    print(schedules)
