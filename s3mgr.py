"""
##            ###########
 ##          ##        ##
  ##        ##         ##
   ##      ##############
    ##    ##
     ##  ##
      ####

AUTHOR = Vimal Paliwal <paliwalvimal1993@gmail.com>
A simple yet useful s3 library. Feel free to make changes according to your requirement.
"""

import boto3
from botocore.exceptions import ClientError

# Important Variables - Do not change the values
STD_STORAGE = "STANDARD"
STD_IA_STORAGE = "STANDARD_IA"
RRS_STORAGE = "REDUCED_REDUNDANCY"

PVT_BUCKET = "private"
PUB_BUCKET = "public-read"
PUB_RW_BUCKET = "public-read-write"

REGION = {
    "N_VIRGINIA": "us-east-1",
    "OHIO": "us-east-2",
    "N_CALIFORNIA": "us-west-1",
    "OREGON": "us-west-2",
    "MUMBAI": "ap-south-1",
    "SEOUL": "ap-northeast-2",
    "SINGAPORE": "ap-southeast-1",
    "SYDNEY": "ap-southeast-2",
    "TOKYO": "ap-northeast-1",
    "CANADA": "ca-central-1",
    "FRANKFURT": "eu-central-1",
    "IRELAND": "eu-west-1",
    "LONDON": "eu-west-2",
    "PARIS": "eu-west-3",
    "SAO_PAULO": "sa-east-1"
}

s3 = boto3.client("s3")


def create_bucket(bucket, region, acl=PVT_BUCKET):
    """
    region: refer to REGION variable
    acl: "PVT_BUCKET"|"PUB_BUCKET"|"PUB_RW_BUCKET"
    """

    try:
        s3.create_bucket(
            Bucket=bucket,
            ACL=acl,
            CreateBucketConfiguration={
                "LocationConstraint": region
            }
        )

        return "0"
    except ClientError as e:
        return e.response['Error']['Code']


def delete_bucket(bucket, force=False):
    """
    force = True(deletes everything inside the bucket before deleting the bucket itself)|False
    """

    try:
        if force:
            empty_bucket(bucket)

        s3.delete_bucket(
            Bucket=bucket
        )

        return "0"
    except ClientError as e:
        return e.response["Error"]["Code"]


def empty_bucket(bucket):
    try:
        response = s3.list_objects_v2(
            Bucket=bucket
        )

        for index in range(0, response["KeyCount"]):
            delete_file(bucket, response["Contents"][index]["Key"])

        return "0"
    except ClientError as e:
        return e.response["Error"]["Code"]


def create_folder(bucket, folder, is_private=True):
    try:
        s3.put_object(
            Bucket=bucket,
            Key=folder + ("/" if folder[-1:] != "/" else ""),
            ACL="private" if is_private else "public-read",
            ContentLength=0
        )
        return "0"
    except ClientError as e:
        return e.response["Error"]["Code"]


def upload_file(bucket, key, file, content_type, storage, encrypt="AES", kms_id="", is_private=True):
    """
    file = full path of file to be uploaded
    content_type = supported MIME type
    encrypt = "AES"|"KMS"
    kms_id = kms key id
    """

    if encrypt == "AES":
        extra_args = {
            "ACL": "private" if is_private else "public-read",
            "ContentType": content_type,
            "StorageClass": storage,
            "ServerSideEncryption": "AES256"
        }
    else:
        extra_args = {
            "ACL": "private" if is_private else "public-read",
            "ContentType": content_type,
            "StorageClass": storage,
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": kms_id,
        }

    try:
        s3.upload_file(
            file, bucket, key,
            ExtraArgs=extra_args
        )

        return "0"
    except ClientError as e:
        return e.response["Error"]["Code"]


def delete_file(bucket, path):
    """
    path = Complete path of key including prefix(if any)
    """

    try:
        s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [
                    {
                        "Key": path
                    }
                ]
            }
        )

        return "0"
    except ClientError as e:
        return e.response["Error"]["Code"]


def list_contents(bucket, path="", include_subdir=True):
    """
    path = list contents of specific folder
    include_subdir = True | False(will only include content of particular folder)
    """

    path = path + ("/" if path != "" and path[-1:] != "/" else "")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=path
    )

    contents = []
    dirs = []
    for i in range(0, response["KeyCount"]):
        if include_subdir:
            if response["Contents"][i]["Key"][-1:] != "/":
                contents.append(response["Contents"][i])
        else:
            if (path.count("/") == response["Contents"][i]["Key"].count("/")) & (response["Contents"][i]["Key"][-1:] != "/"):
                contents.append(response["Contents"][i])
            elif (((path.count("/")+1) == response["Contents"][i]["Key"].count("/")) &
                    (response["Contents"][i]["Key"][0:response["Contents"][i]["Key"].find("/", response["Contents"][i]["Key"].find("/") + (-1 if path == "" else 1)) + 1] not in dirs)):
                dirs.append(response["Contents"][i]["Key"])
                dirs[len(dirs)-1] = dirs[len(dirs)-1][0:dirs[len(dirs)-1].find("/", dirs[len(dirs)-1].find("/") + (-1 if path == "" else 1)) + 1]

    return {
        "Files": contents,
        "Dirs": dirs
    }
