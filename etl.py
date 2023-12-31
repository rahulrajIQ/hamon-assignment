import pandas as pd
import datetime as dt
import os
import io
import logging
import psycopg2
from sqlalchemy import create_engine
import localstack_client.session as boto3
from io import StringIO


logging.basicConfig(filename='app.log', filemode='a', format='%(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

data_list = ['customer_data', 'booking_data', 'destination_data']
chunk_size = 50000

#Downloading customer_data.csv
try:
    url='https://drive.google.com/file/d/14ptTEbFPuyjN520Zkaqyo1lmotoOgdU2/view?usp=sharing'
    url='https://drive.google.com/uc?id=' + url.split('/')[-2]

    for i, data in enumerate(pd.read_csv(url,chunksize = chunk_size)):
        df = data
        if df.shape[0] < chunk_size and i == 0:
            df.to_csv('customer_data/customer_data'+ '.csv', index = False)
        else:
            df.to_csv('customer_data/customer_data'+ '_' + str(i)+'.csv', index = False)

except Exception as e:
    logging.error("Exception occurred for customer_data", exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

#Downloading booking_data.csv
try:
    url='https://drive.google.com/file/d/1ZZDKAjEUv0-EIn1Dh5MITcIMOz-lUJOr/view?usp=sharing'
    url='https://drive.google.com/uc?id=' + url.split('/')[-2]

    for i, data in enumerate(pd.read_csv(url,chunksize = chunk_size)):
        df = data
        if df.shape[0] < chunk_size and i == 0:
            df.to_csv('booking_data/booking_data'+ '.csv', index = False)
        else:
            df.to_csv('booking_data/booking_data'+ '_' + str(i)+'.csv', index = False)

except Exception as e:
    logging.error("Exception occurred for booking_data", exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")


#Downloading destination_data.csv
try:
    url='https://drive.google.com/file/d/15pM-pn9hXu8gUxY2PhglzI8BMPNj_hPr/view?usp=sharing'
    url='https://drive.google.com/uc?id=' + url.split('/')[-2]

    for i, data in enumerate(pd.read_csv(url,chunksize = chunk_size)):
        df = data
        if df.shape[0] < chunk_size and i == 0:
            df.to_csv('destination_data/destination_data'+'.csv', index = False)
        else:
            df.to_csv('destination_data/destination_data'+ '_' + str(i)+'.csv', index = False)

except Exception as e:
    logging.error("Exception occurred for destination_data", exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")



# DDL for the tables

ddl = ['''CREATE TABLE customer_data (
  customer_id varchar NOT NULL UNIQUE,
  first_name varchar NOT NULL,
  last_name varchar,
  email varchar,
  phone varchar
);''',
'''CREATE TABLE booking_data (
  booking_id varchar NOT NULL UNIQUE,
  customer_id varchar NOT NULL,
  booking_date timestamp,
  destination varchar,
  number_of_passengers int,
  cost_per_passenger float,
  total_booking_value float,
  FOREIGN KEY (customer_id) REFERENCES customer_data(customer_id)
);''', 
       '''CREATE TABLE destination_data (
  destination_id varchar NOT NULL UNIQUE,
  destination varchar,
  country varchar,
  popular_season varchar
);''']

# Establishing Postgres connection
try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres")

    cursor = conn.cursor()
    
except Exception as e:
    logging.error("Exception occurred during postgres connection", exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")



# Creating table in Postgres if not already exists
def create_table(ddl, table_name, cursor,conn):
    try:
        cursor.execute('''SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = '{}'
        ) AS table_existence;'''.format(table_name))

        if cursor.fetchone()[0]== True:
            pass
        else:
            cursor.execute(ddl)
            conn.commit()
            
    except Exception as e:
        err_str = "Exception occurred for ddl execution of table - " + table_name
        logging.error(err_str, exc_info=True)
        logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    

for i,j in enumerate(data_list):
    create_table(ddl[i],j,cursor,conn)



# A short Transform function
def transform(df, files):
    df= df.fillna('')
    if files == 'booking_data':
        df.booking_date = pd.to_datetime(df.booking_date)
        df['total_booking_value'] = df.number_of_passengers * df.cost_per_passenger
        
    return df


## Creating SQLAlchemy engine for Postgrs db for loading.
try:
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
except Exception as e:
    err_str = "Exception occurred during create_engine"
    logging.error(err_str, exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")


#ETL - Loading to Postgres db
try:
    for i in data_list:
        all_files = os.listdir(i+"/")    
        csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
        for j in csv_files:
            df= pd.read_csv(i+'/'+j)
            df = transform(df, i)
            df.to_sql(i, engine, index= False, if_exists = 'append')
            
except Exception as e:
    err_str = "Exception occurred during loading"
    logging.error(err_str, exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")



# Batch transfer to S3
BUCKET = 'travel-bucket'
s3 = boto3.resource('s3')
try:
    for i in data_list:
        all_files = os.listdir(i+"/")    
        csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
        for j in csv_files:
            s3.Bucket(BUCKET).upload_file(Filename= i+'/'+ j, Key= i+'/'+ j)
            
            
except Exception as e:
    err_str = "Exception occurred during S3 transfer"
    logging.error(err_str, exc_info=True)
    logging.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")


