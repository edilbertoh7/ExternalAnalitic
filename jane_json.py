import boto3


client = boto3.client("s3")
bucket = "changeactivebucket"
key = "changestudents/empresas.json"
expression_type = "SQL"
expression = """SELECT * FROM S3Object[*]"""
input_serialization = {"JSON": {"Type": "Document"}}
output_serialization = {"JSON": {}}
response = client.select_object_content(
    Bucket=bucket,
    Key=key,
    ExpressionType=expression_type,
    Expression=expression,
    InputSerialization=input_serialization,
    OutputSerialization=output_serialization
)
for event in response["Payload"]:
    if 'Records' in event:
        records = event['Records']['Payload'].decode('utf-8')
    print(records)

