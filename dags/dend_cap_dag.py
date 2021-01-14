# Instructions
# Define a function that uses the python logger to log a function. Then finish filling in the details of the DAG down below. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file or the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
import sql_queries as sq

    
def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sq.load_data_from_S3.format(kwargs['table_name'], kwargs['s3_url']))

def check_tables(*args, **kwargs):
    table_name = kwargs['table_name']
    redshift_hook = PostgresHook("redshift")
    #Count rows
    query = "SELECT COUNT(*) FROM {}".format(table_name)
    logging.info("Executing a query: {}".format(query))
    records = redshift_hook.get_records(query)
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table_name} returned no results")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError('Not a single record was found in this step')
    logging.info(f"Data quality on table {table_name} check passed with {records[0][0]} records")
    
    #Show the first 5 rows
    query = "SELECT * FROM {} LIMIT 5".format(table_name)
    logging.info("Executing a query: {}".format(query))
    records = redshift_hook.get_records(query)
    for rec in records:
        logging.info(rec)

    
    
dag = DAG(
        'etl_process',
        start_date=datetime.datetime.now())

#Tasks

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.drop_then_create_tables_queries
)

load_liquor_data_task = PythonOperator(
    task_id="load_liquor_data",
    python_callable=load_data_to_redshift,
    op_kwargs={'table_name': 'staging_liquor_sales',
               's3_url': "'s3://myawsbucket20201109/Iowa_Liquor_Sales_small.csv'"},
    dag=dag
)

check_load_liquor_task = PythonOperator(
    task_id="check_load_liquor",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'staging_liquor_sales'}
)


load_census_data_task = PythonOperator(
    task_id="load_census_data",
    python_callable=load_data_to_redshift,
    op_kwargs={'table_name': 'staging_census',
               's3_url': "'s3://myawsbucket20201109/acs2017_county_data.csv'"},
    dag=dag
)

check_load_census_task = PythonOperator(
    task_id="check_load_census",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'staging_census'}
)


load_crime_data_task = PythonOperator(
    task_id="load_crime_data",
    python_callable=load_data_to_redshift,
    op_kwargs={'table_name': 'staging_crime',
               's3_url': "'s3://myawsbucket20201109/crime_data_w_population_and_crime_rate.csv'"},
    dag=dag
)

check_load_crime_task = PythonOperator(
    task_id="check_load_crime",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'staging_crime'}
)


load_temperature_data_task = PythonOperator(
    task_id="load_temperature_data",
    python_callable=load_data_to_redshift,
    op_kwargs={'table_name': 'staging_temperature',
               's3_url': "'s3://myawsbucket20201109/city_temperature_small.csv'"},
    dag=dag
)

check_load_temperature_task = PythonOperator(
    task_id="check_load_temperature",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'staging_temperature'}
)


load_to_insert_dummy = DummyOperator(task_id='load_to_insert',  dag=dag)

insert_sales_fact_task = PostgresOperator(
    task_id="insert_sales_fact_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.sales_fact_table_insert
)

check_insert_sales_fact_task = PythonOperator(
    task_id="check_insert_sales_fact",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'sales_fact'}
)


insert_county_census_dim_task = PostgresOperator(
    task_id="insert_county_census_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.county_census_dim_table_insert
)


check_insert_county_census_dim_task = PythonOperator(
    task_id="check_insert_county_census_dim",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'county_census_dim'}
)

insert_item_dim_task = PostgresOperator(
    task_id="insert_item_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.item_dim_table_insert
)

check_insert_item_dim_task = PythonOperator(
    task_id="check_insert_item_dim",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'item_dim'}
)


insert_store_dim_task = PostgresOperator(
    task_id="insert_store_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.store_dim_table_insert
)

check_insert_store_dim_task = PythonOperator(
    task_id="check_insert_store_dim",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'store_dim'}
)

insert_temperature_dim_task = PostgresOperator(
    task_id="insert_temperature_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.temperature_dim_table_insert
)

check_insert_temperature_dim_task = PythonOperator(
    task_id="check_insert_temperature_dim",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'temperature_dim'}
)

insert_time_dim_task = PostgresOperator(
    task_id="insert_time_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql= sq.time_dim_table_insert
)

check_insert_time_dim_task = PythonOperator(
    task_id="check_insert_time_dim",
    dag=dag,
    python_callable=check_tables,
    op_kwargs = {'table_name': 'county_census_dim'}
)

end = DummyOperator(task_id='end',  dag=dag)


load_data_tasks = [load_liquor_data_task,
                   load_census_data_task, 
                   load_crime_data_task, 
                   load_temperature_data_task]

check_load_tasks = [check_load_liquor_task,
                   check_load_census_task, 
                   check_load_crime_task, 
                   check_load_temperature_task]

insert_tasks = [insert_sales_fact_task, 
                insert_county_census_dim_task,
                insert_item_dim_task,
                insert_store_dim_task,
                insert_temperature_dim_task,
                insert_temperature_dim_task,
                insert_time_dim_task]

check_insert_tasks = [check_insert_sales_fact_task, 
                      check_insert_county_census_dim_task,
                      check_insert_item_dim_task,
                      check_insert_store_dim_task,
                      check_insert_temperature_dim_task,
                      check_insert_temperature_dim_task,
                      check_insert_time_dim_task]

create_tables_task >> load_data_tasks

load_liquor_data_task      >> check_load_liquor_task     
load_census_data_task      >> check_load_census_task 
load_crime_data_task       >> check_load_crime_task 
load_temperature_data_task >> check_load_temperature_task

check_load_tasks >> load_to_insert_dummy >> insert_tasks 

insert_sales_fact_task        >> check_insert_sales_fact_task,    
insert_county_census_dim_task >> check_insert_county_census_dim_task,   
insert_item_dim_task          >> check_insert_item_dim_task,  
insert_store_dim_task         >> check_insert_store_dim_task, 
insert_temperature_dim_task   >> check_insert_temperature_dim_task, 
insert_temperature_dim_task   >> check_insert_temperature_dim_task,
insert_time_dim_task          >> check_insert_time_dim_task  

check_insert_tasks >> end