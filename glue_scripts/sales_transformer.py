import sys
import boto3
import pandas as pd
from awsglue.utils import getResolvedOptions
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['INPUT_PATH', 'OUTPUT_PATH', 'REDSHIFT_CONNECTION'])

s3 = boto3.client('s3')
glue = boto3.client('glue')

def extract_bucket_and_key(s3_path):
    """Extract bucket and key from S3 path"""
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts[0]
    key = "/".join(path_parts[1:])
    return bucket, key

def transform_data(df):
    """Apply transformations to the dataframe"""
    # Clean product IDs
    df['product_id'] = df['product_id'].str.upper().str.strip()
    
    # Clean customer IDs
    df['customer_id'] = df['customer_id'].str.replace('-', '').str.zfill(10)
    
    # Calculate total sale
    df['total_sale'] = df['unit_price'].astype(float) * df['quantity'].astype(int)
    
    # Convert date columns
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['processed_at'] = datetime.now()
    
    # Filter invalid records
    df = df[(df['order_date'].notna()) & (df['total_sale'] > 0)]
    
    return df

def load_to_redshift(df, connection_name, table_name):
    """Load transformed data to Redshift"""
    # Create a Glue connection
    connection = glue.get_connection(Name=connection_name)
    connection_properties = connection['Connection']['ConnectionProperties']
    
    # SQLAlchemy connection string
    conn_str = f"postgresql+psycopg2://{connection_properties['USERNAME']}:{connection_properties['PASSWORD']}@" \
               f"{connection_properties['JDBC_CONNECTION_URL'].split('//')[1].split('/')[0]}/" \
               f"{connection_properties['JDBC_CONNECTION_URL'].split('/')[-1].split('?')[0]}"
    
    # Write to Redshift
    df.to_sql(
        table_name,
        conn_str,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )

def main():
    # Extract data from S3
    input_bucket, input_key = extract_bucket_and_key(args['INPUT_PATH'])
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(obj['Body'])
    
    # Transform data
    transformed_df = transform_data(df)
    
    # Load to Redshift
    load_to_redshift(transformed_df, args['REDSHIFT_CONNECTION'], 'sales_fact')
    
    # Write processed file to output location (optional)
    output_bucket, output_key = extract_bucket_and_key(args['OUTPUT_PATH'])
    transformed_df.to_csv(f's3://{output_bucket}/{output_key}/processed_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv', index=False)

if __name__ == '__main__':
    main()