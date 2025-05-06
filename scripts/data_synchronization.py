import os
import urllib.request
from datetime import datetime
import boto3

def data_synchronization(event, context):
    # Retrieve environment variables (with defaults for local testing)
    api_url = os.environ.get('API_URL', 'https://example.com/api')  # Default for local testing
    bucket_name = os.environ.get('BUCKET_NAME', 'test-bucket')  # Default for local testing

    try:
        # Fetch data from the API
        with urllib.request.urlopen(api_url) as response:
            # Ensure the response is text/csv
            content_type = response.headers.get('Content-Type', '')
            if 'text/csv' not in content_type.lower():
                raise ValueError(f"Expected text/csv, got {content_type}")
            csv_data = response.read().decode('utf-8')
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            file_path = f'daa_market/{current_year}/{current_month}/daa_market.csv'
            if 'AWS_LAMBDA_FUNCTION_NAME' not in os.environ:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(csv_data)
                print(f"Saved CSV locally as {file_path}")
            else:  # AWS Lambda execution
                s3 = boto3.client('s3')
                s3.put_object(Body=csv_data, Bucket=bucket_name, Key=file_path)

            return {
                'statusCode': 200,
                'body': 'Successfully processed'
            }
    except Exception as e:
        # Basic error logging
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': 'Failed to process'
        }



if __name__ == "__main__":
    # Simulate environment variables
    os.environ['API_URL'] = 'https://run.mocky.io/v3/c74eb710-9a88-4c57-8d59-03639f16ad6f'
    os.environ['BUCKET_NAME'] = 'test-bucket'

    # Simulate an event and context (can be empty for this case)
    test_event = {}
    test_context = None

    # Run the Lambda handler
    result = data_synchronization(test_event, test_context)