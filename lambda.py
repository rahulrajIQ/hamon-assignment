
import localstack_client.session as boto3
import pandas as pd
import io
from io import StringIO
import logging
logger = logging.getLogger()
logger.setLevel("INFO")

def lambda_handler(event, context):
    try:
        s3 = boto3.resource('s3')
        BUCKET = 'travel-bucket'
        csv_files=[]
        s3_bucket = s3.Bucket(BUCKET)
        for obj in s3_bucket.objects.all():
                if obj.key.split('/')[0]== 'booking_data':
                        csv_files.append(obj.key)
                
        
        s3_client = boto3.client('s3')
        for i, file_path in enumerate(csv_files):
                # Read the CSV file from S3
                response = s3_client.get_object(Bucket=BUCKET, Key= file_path)
                csv_content = response['Body'].read().decode('utf-8')

                # Create a Pandas DataFrame
                df_booking_data = pd.read_csv(io.StringIO(csv_content))
                
                if i==0:
                        df = df_booking_data.copy()    
                else:
                        df = pd.concat([df,df_booking_data])

                
        df['total_revenue_per_destination'] = df.number_of_passengers * df.cost_per_passenger
        dff = df.groupby(by='destination').agg({'booking_id':'count', 'total_revenue_per_destination': 'sum'})
        dff = dff.rename(columns={'booking_id':'total_bookings_per_destination'}).reset_index()
        
        csv_buffer = StringIO()
        dff.to_csv(csv_buffer)
        s3.Object(BUCKET, 'total_bookings_per_destination.csv').put(Body=csv_buffer.getvalue())

    except Exception as e:
        err_str = "Exception occurred"
        logging.info(err_str)

    