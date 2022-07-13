from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
import pathlib

default_args = {"owner":"airflow", "start_date":datetime(2022,7,9)}

def id_file():
    
    ruta = pathlib.Path("../ip_files")
    archivos_csv = []
    for archivo in ruta.glob("*.csv"):
        archivos_csv.append(archivo)

    olist_customers_dataset = olist_geolocation_dataset = olist_order_items_dataset = olist_order_payments_dataset = False
    olist_order_reviews_dataset = olist_orders_dataset = olist_products_dataset = olist_sellers_dataset = False
    product_category_name_translation = olist_closed_deals_dataset = olist_marketing_qualified_leads_dataset = False

    for i in archivos_csv:
        if "customers" in str(i).lower():
            olist_customers_dataset = True
        if "geolocation" in str(i).lower():
            olist_geolocation_dataset = True
        if "order_items" in str(i).lower():
            olist_order_items_dataset = True
        if "order_payments" in str(i).lower():
            olist_order_payments_dataset = True
        if "order_reviews" in str(i).lower():
            olist_order_reviews_dataset = True
        if "orders" in str(i).lower():
            olist_orders_dataset = True
        if "products" in str(i).lower():
            olist_products_dataset = True
        if "sellers" in str(i).lower():
            olist_sellers_dataset = True
        if "product_category_name_translation" in str(i).lower():
            product_category_name_translation = True
        if "closed_deals" in str(i).lower():
            olist_closed_deals_dataset = True
        if "marketing_qualified_leads" in str(i).lower():
            olist_marketing_qualified_leads_dataset = True

    list_flags = [olist_customers_dataset, olist_geolocation_dataset, olist_order_items_dataset, 
                 olist_order_payments_dataset, olist_order_reviews_dataset, olist_orders_dataset,
                 olist_products_dataset, olist_sellers_dataset, product_category_name_translation,
                 olist_closed_deals_dataset, olist_marketing_qualified_leads_dataset]
    
    return list_flags







with DAG(dag_id="workflow_proyet", default_args=default_args, schedule_interval="@daily") as dag:
    
    id_file = PythonOperator(
        task_id = "id_file",
        python_callable = id_file
        )

    id_file >> 