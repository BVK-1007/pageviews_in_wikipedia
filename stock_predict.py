from urllib import request
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sqlite3
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

connection=sqlite3.connect('page_view_counts.db')
cursor=connection.cursor()
command="""CREATE TABLE IF NOT EXISTS pagecounts(pagename VARCHAR(50) NOT NULL,pageviewct INT NOT NULL,dates TIMESTAMP NOT NULL);"""
cursor.execute(command)

def _fetch_page_views(pagenames,execution_date,**_):
    result=dict.fromkeys(pagenames,0)
    with open(f"/tmp/wikipageviews","r") as f:
        for line in f:
            domain_code,page_title,view_counts,_=line.split(" ")
            if domain_code=="en" and page_title in pagenames:
                result[page_title]=view_counts
    with open("/tmp/postgres_query.sql","w") as f:
        for pagename,pageviewcount in result.items():
            f.write("INSERT INTO pagecounts VALUES ("f" '{pagename}','{pageviewcount}','{execution_date}'"");\n")
    

def _get_data(year,month,day,hour,output_path,**_):
    url=("https://dumps.wikimedia.org/other/pageviews/"f"{year}/{year}-{month:0>2}/"f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz")
    request.urlretrieve(url,output_path)

with DAG(dag_id='stocker',start_date=datetime(2023,8,12),schedule="@hourly",template_searchpath="/tmp") as dag:

    get_data=PythonOperator(task_id='get_data',python_callable=_get_data,op_kwargs={"year":"{{execution_date.year}}","month":"{{execution_date.month}}","day":"{{execution_date.day}}","hour":"{{execution_date.hour}}","output_path":'/tmp/wikipageviews.gz'})

    extract_gz=BashOperator(task_id='extract_gz',bash_command="gunzip --force /tmp/wikipageviews.gz")

    fetch_pageviews=PythonOperator(task_id="fetch_pageviews",python_callable=_fetch_page_views,op_kwargs={"pagenames":{"Google","Amazon","Apple","Microsoft","Meta",}})

    write_to_postgres=PostgresOperator(task_id="write_to_postgres",sql="postgres_query.sql")

    get_data >> extract_gz >> fetch_pageviews >> write_to_postgres