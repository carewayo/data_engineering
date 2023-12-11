#### Aim: 
The Aim of this project is to create an ETL script that can extract the list of Universities in Nigeria, create and fill a column for the fees based on the type of University it is. 

#### Tools: 
To achieve this project, I used python to create a scrpt which did the extraction of the tables from the website. Then I utilized pandas to manipulate the data and transform the data as the quantity of data involved is not large. To load the transformed data into the Postgres Database, local-drive and aws data-lake, I wrote a python function which made us of psycopg2 to connect and transfer data into the Postgres Database. Boto3 SDk was used for the AWS Data-Lake loading and pandas function to_csv was used to save the backup on local-drive path. 

#### Improvement:
Further modification of the data is expected to be done within the database and further transformation will be done in the Data-WareHouse on AWS RedShift. 