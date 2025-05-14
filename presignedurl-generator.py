#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 12 16:29:44 2025

@author: sasidharankumar
"""

import json
import os
import boto3
import uuid
from datetime import datetime

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

S3_BUCKET = os.environ['S3_BUCKET']
UPLOAD_TABLE = os.environ['UPLOADS_TABLE']

uploads_table = dynamodb.Table(UPLOAD_TABLE)

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        file_name = body.get('fileName')
        file_type = body.get('fileType')
        metadata = body.get('metadata', {})

        if not file_name or not file_type or not metadata:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'fileName, fileType, and metadata are required'})
            }

        # Generate unique transaction ID
        transaction_id = str(uuid.uuid4())

        # Define S3 object key (path)
        s3_key = f"uploads/{transaction_id}/{file_name}"

        # Prepare Metadata headers (S3 expects lowercase keys)
        metadata_headers = {k.lower(): str(v) for k, v in metadata.items()}

        # Generate pre-signed PUT URL with metadata
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': s3_key,
                'ContentType': file_type,
                'Metadata': metadata_headers
            },
            ExpiresIn=900  # URL expires in 15 minutes
        )

        # Store audit info in DynamoDB
        uploads_table.put_item(Item={
            'transactionId': transaction_id,
            'fileName': file_name,
            'fileType': file_type,
            's3Key': s3_key,
            'metadata': metadata,
            'status': 'PENDING',
            'createdAt': datetime.utcnow().isoformat()
        })

        return {
            'statusCode': 200,
            'body': json.dumps({
                'transactionId': transaction_id,
                'signedUrl': presigned_url
            })
        }

    except Exception as e:
        print(f"Error generating presigned URL: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }
