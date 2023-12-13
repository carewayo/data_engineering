#pip3 install pandas requests beautifulsoup4 sqlalchemy lxml
import pandas as pd
import requests
import boto3
import psycopg2
import os

from configparser import ConfigParser
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from io import StringIO

config = ConfigParser()
config.read('.env')

#Calling keys from the .env using configparser function
region = config['AWS']['region']
access_key = config['AWS']['aws_access_key']
secret_key = config['AWS']['aws_secret_access_key']
bucket_name = config['AWS']['bucketname']

host = config['DB_CRED']['host']
db_username = config['DB_CRED']['db_username']
db_password = config['DB_CRED']['db_password']
db_name = config['DB_CRED']['db_name']
port = config['DB_CRED']['port']



# S3 bucket and object details
s3_bucket = 'ayodevops-gitlab'
s3_folder = 'webscraper'
file_name = 'transformed_ngn_universities.csv'


def extract_data():
    data = pd.DataFrame()
    url = 'https://en.wikipedia.org/wiki/List_of_universities_in_Nigeria'
    scrapped_data = requests.get(url)
    scrapped_data = scrapped_data.content
    # print(scrapped_data)
    #To trim the scrapped_data content and parse into lxml format
    soup = BeautifulSoup(scrapped_data, 'lxml')  #This is a parser
    #print(soup)
    html_data = str(soup.find_all('table'))
    # print(html_data)
    #The step next is as a result of a warning of deprecated method of passing html directly to pd
    #Instead we created a pd.read_html function with a StringIO object which is valid
    #html_data_io = StringIO(html_data)
    # df1 = pd.read_html(html_data)[0]
    # df2 = pd.read_html(html_data)[2]
    # df3 = pd.read_html(html_data)[6]
    df = pd.read_html(html_data)[0:7]
    result_df = pd.concat(df)
    #print(df3.head()) #To see what the content of the subset dataframes look like
    # result_df = pd.concat([df1, df2, df3], axis=0, ignore_index=True) #Merging all 3 df into a long df
    result_df.to_csv('data/raw_ngn_universities.csv', index=False) #Convert result_df into csv and load to local-drive
    print(result_df) #For debugging to make sure the result_df contains Federal, State and Private universities
    print('Data successfully merged and written to a csv file')


# Data Loading Transformation Layer
def transform_data():
    data = pd.read_csv('data/raw_ngn_universities.csv')  #Read csv file from local-drive
    def get_fees(value):
        if value == 'State':
            return '350,000'
        elif value == 'Federal':
            return '120,000'
        elif value == 'Private':
            return '850,000'
        else:
            return 'No fees'
    data['Fee'] = data['Funding'].apply(get_fees)
    data = data[['Name', 'State', 'Abbreviation', 'Location', 'Funding', 'Fee', 'Founded']]
    data.to_csv('data/transformed_ngn_universities.csv', index= False)
    print('Data transformed and written to a csv file')

# Data loading layer
def load_to_db():
    data = pd.read_csv('data/transformed_ngn_universities.csv') # Read csv file
    engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{host}:{port}/{db_name}')
    data.to_sql('ngn_universities', con= engine, if_exists='replace', index= False)
    print('Data successfully written to PostgreSQL database')

# Loading the transformed data to AWS Data-Lake
#Configuring boto3 to use the credentials from .env file
boto3.setup_default_session(
    aws_access_key_id=config['AWS']['aws_access_key'],
    aws_secret_access_key=config['AWS']['aws_secret_access_key'],
    region_name=config['AWS']['region']
)

def upload_to_s3():
    s3 = boto3.client('s3')
    s3.upload_file('data/transformed_ngn_universities.csv', s3_bucket, f'{s3_folder}/transformed_ngn_universities.csv')
    print('CSV file successfully uploaded to S3')


extract_data()
transform_data()
load_to_db()
upload_to_s3()