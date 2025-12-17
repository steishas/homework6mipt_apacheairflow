from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,  # используется e-mail заглушка
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test_user@example.com'],  # тестовый e-mail
}

# Пути к CSV файлам
CSV_PATHS = {
    'customer': '/opt/airflow/dags/data/customer.csv',
    'product': '/opt/airflow/dags/data/product.csv',
    'orders': '/opt/airflow/dags/data/orders.csv',
    'order_items': '/opt/airflow/dags/data/order_items.csv',
}

# Пути для выгрузки результатов
OUTPUT_PATHS = {
    'query1': '/opt/airflow/dags/results/top_customers.csv',
    'query2': '/opt/airflow/dags/results/wealth_segment_top5.csv',
}


def save_query_result_to_csv(query_sql, output_path, **kwargs):
    """Выполняет SQL запрос и сохраняет результат в CSV"""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = hook.get_pandas_df(sql=query_sql)
        df.to_csv(output_path, index=False, encoding='utf-8')

        ti = kwargs['ti']
        ti.xcom_push(key=f'row_count_{os.path.basename(output_path)}', value=len(df))

        logging.info(f"Сохранено {len(df)} строк в {output_path}")
        return len(df)
    except Exception as e:
        logging.error(f"Ошибка: {str(e)}")
        raise


def check_query_results(**kwargs):
    """Проверяет, что запросы вернули ненулевое количество строк"""
    ti = kwargs['ti']

    count1 = ti.xcom_pull(key='row_count_top_customers.csv', task_ids='execute_and_save_query1')
    count2 = ti.xcom_pull(key='row_count_wealth_segment_top5.csv', task_ids='execute_and_save_query2')

    logging.info(f"Query1: {count1} строк, Query2: {count2} строк")

    if count1 is not None and count2 is not None and count1 > 0 and count2 > 0:
        logging.info("Все запросы вернули данные - DAG завершен успешно")
        ti.xcom_push(key='dag_status', value='success')
        ti.xcom_push(key='dag_message', value='DAG выполнен успешно! Все запросы вернули данные.')
        return 'end'  # Просто завершаем DAG
    else:
        logging.error("Один или все результаты запросов пустые - отправляем email")
        ti.xcom_push(key='dag_status', value='error')
        ti.xcom_push(key='dag_message', value='Один или все результаты запросов пустые.')
        return 'log_error_email'  # Отправляем email только при ошибке

def log_error_email(**kwargs):
    """Логирует email об ошибке (заглушка)"""
    print("\n" + "=" * 60)
    print(" [EMAIL ЗАГЛУШКА] ОШИБКА ВЫПОЛНЕНИЯ")
    print("=" * 60)
    print("Кому: test_user@example.com")
    print("Тема: Airflow DAG - ошибка")
    print("Сообщение: Один или все результаты запросов пустые")
    print("=" * 60 + "\n")

    return "error_email_logged"


def load_data_via_pandas(table_name, csv_path, **kwargs):
    """Загружает данные из CSV в PostgreSQL с помощью pandas"""
    try:
        import pandas as pd
        from sqlalchemy import create_engine

        logging.info(f"Начинаю загрузку {csv_path} в таблицу {table_name}")

        # Подключение к PostgreSQL
        engine = create_engine('postgresql://airflow:airflow@postgres/airflow')

        # Определяем разделитель
        with open(csv_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()

        sep = ';' if ';' in first_line else ','

        # Читаем CSV
        df = pd.read_csv(csv_path, sep=sep)

        # Приводим названия столбцов к нижнему регистру
        df.columns = [col.lower() for col in df.columns]

        logging.info(f"Прочитано {len(df)} строк из {csv_path}")
        logging.info(f"Столбцы (нижний регистр): {list(df.columns)}")

        # Загрузка в БД
        df.to_sql(table_name, engine, if_exists='append', index=False)

        logging.info(f"Успешно загружено {len(df)} строк в таблицу {table_name}")

        # Возвращаем количество загруженных строк
        ti = kwargs['ti']
        ti.xcom_push(key=f'loaded_rows_{table_name}', value=len(df))

        return len(df)
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных в {table_name}: {str(e)}")
        raise


with DAG(
        'homework_mipt_6_dag',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=False,
        tags=['postgres', 'final', 'mock_email'],
        max_active_tasks=10,
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # 1. СОЗДАНИЕ ТАБЛИЦ

    create_customer_table = PostgresOperator(
        task_id='create_customer_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS customer CASCADE;
            CREATE TABLE customer (
                customer_id integer PRIMARY KEY,
                first_name text,
                last_name text,
                gender text,
                DOB date,
                job_title text,
                job_industry_category text,
                wealth_segment text,
                deceased_indicator text,
                owns_car text,
                address text,
                postcode varchar,
                state text,
                country text,
                property_valuation integer
            );
        """
    )

    create_product_table = PostgresOperator(
        task_id='create_product_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS product CASCADE;
            CREATE TABLE product (
                product_id integer,
                brand text,
                product_line text,
                product_class text,
                product_size text,
                list_price numeric(10,2),
                standard_cost numeric(10,2)
            );
        """
    )

    create_orders_table = PostgresOperator(
        task_id='create_orders_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS orders CASCADE;
            CREATE TABLE orders (
                order_id integer PRIMARY KEY,
                customer_id integer,
                order_date date,
                online_order boolean,
                order_status text
            );
        """
    )

    create_order_items_table = PostgresOperator(
        task_id='create_order_items_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS order_items CASCADE;
            CREATE TABLE order_items (
                order_item_id integer PRIMARY KEY,
                product_id integer,
                order_id integer,
                quantity integer,
                item_list_price_at_sale numeric(10,2),
                item_standard_cost_at_sale numeric(10,2)
            );
        """
    )

    # 2. ЗАГРУЗКА ДАННЫХ

    load_customer_data = PythonOperator(
        task_id='load_customer_data',
        python_callable=load_data_via_pandas,
        op_kwargs={
            'table_name': 'customer',
            'csv_path': CSV_PATHS['customer']
        }
    )

    load_product_data = PythonOperator(
        task_id='load_product_data',
        python_callable=load_data_via_pandas,
        op_kwargs={
            'table_name': 'product',
            'csv_path': CSV_PATHS['product']
        }
    )

    load_orders_data = PythonOperator(
        task_id='load_orders_data',
        python_callable=load_data_via_pandas,
        op_kwargs={
            'table_name': 'orders',
            'csv_path': CSV_PATHS['orders']
        }
    )

    load_order_items_data = PythonOperator(
        task_id='load_order_items_data',
        python_callable=load_data_via_pandas,
        op_kwargs={
            'table_name': 'order_items',
            'csv_path': CSV_PATHS['order_items']
        }
    )

    # 3. ОБРАБОТКА PRODUCT

    create_product_corr_table = PostgresOperator(
        task_id='create_product_corr_table',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS product_cor;
            CREATE TABLE product_cor AS
            SELECT DISTINCT ON (product_id) *
            FROM product
            ORDER BY product_id, list_price DESC;
            ALTER TABLE product_cor ADD PRIMARY KEY (product_id);
        """
    )

    # 4. SQL ЗАПРОСЫ

    QUERY_1 = """
    WITH customer_sales AS (
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) AS total_sales
        FROM customer c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_id, c.first_name, c.last_name
    ),
    ranked_customers AS (
        SELECT
            first_name,
            last_name,
            total_sales,
            ROW_NUMBER() OVER (ORDER BY total_sales ASC) AS lowest_rank,
            ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS highest_rank
        FROM customer_sales
    )
    SELECT 
        first_name,
        last_name,
        total_sales,
        'Минимум' as type
    FROM ranked_customers
    WHERE lowest_rank <= 3
    UNION ALL
    SELECT 
        first_name,
        last_name,
        total_sales,
        'Максимум' as type
    FROM ranked_customers
    WHERE highest_rank <= 3
    ORDER BY type, total_sales;
    """

    QUERY_2 = """
    WITH customer_info AS (
        SELECT
            ROW_NUMBER() OVER(PARTITION BY wealth_segment ORDER BY revenue_generated DESC) AS top_position,
            first_name,
            last_name,
            wealth_segment,
            revenue_generated
        FROM (
            SELECT
                c.customer_id,
                wealth_segment,
                SUM(quantity * oi.item_list_price_at_sale) AS revenue_generated,
                first_name,
                last_name
            FROM order_items AS oi
            JOIN orders AS o ON oi.order_id = o.order_id 
            JOIN customer AS c ON o.customer_id = c.customer_id 
            GROUP BY c.customer_id, wealth_segment
        ) sub
    )
    SELECT
        first_name,
        last_name,
        wealth_segment,
        revenue_generated,
        top_position
    FROM customer_info
    WHERE top_position <= 5
    ORDER BY wealth_segment, top_position;
    """

    execute_and_save_query1 = PythonOperator(
        task_id='execute_and_save_query1',
        python_callable=save_query_result_to_csv,
        op_kwargs={
            'query_sql': QUERY_1,
            'output_path': OUTPUT_PATHS['query1']
        }
    )

    execute_and_save_query2 = PythonOperator(
        task_id='execute_and_save_query2',
        python_callable=save_query_result_to_csv,
        op_kwargs={
            'query_sql': QUERY_2,
            'output_path': OUTPUT_PATHS['query2']
        }
    )

    # 5. ПРОВЕРКА РЕЗУЛЬТАТОВ
    check_results = BranchPythonOperator(
        task_id='check_results',
        python_callable=check_query_results
    )

    # 6. EMAIL ЗАГЛУШКА ТОЛЬКО ДЛЯ ОШИБОК
    log_error_email_task = PythonOperator(
        task_id='log_error_email',
        python_callable=log_error_email
    )

    # 7. СИНХРОНИЗАЦИОННЫЕ DummyOperator

    tables_created = DummyOperator(task_id='tables_created')
    all_data_loaded = DummyOperator(task_id='all_data_loaded')

    # ПОРЯДОК ВЫПОЛНЕНИЯ ТАСКОВ

    # Шаг 1: Параллельное создание таблиц
    start >> create_customer_table >> tables_created
    start >> create_product_table >> tables_created
    start >> create_orders_table >> tables_created
    start >> create_order_items_table >> tables_created

    # Шаг 2: Загрузка данных
    tables_created >> load_customer_data >> all_data_loaded
    tables_created >> load_product_data >> all_data_loaded
    tables_created >> load_orders_data >> all_data_loaded
    tables_created >> load_order_items_data >> all_data_loaded

    # Шаг 3: Обработка product
    load_product_data >> create_product_corr_table >> all_data_loaded

    # Шаг 4: Выполнение запросов
    all_data_loaded >> execute_and_save_query1
    all_data_loaded >> execute_and_save_query2

    # Шаг 5: Проверка результатов
    execute_and_save_query1 >> check_results
    execute_and_save_query2 >> check_results

    # Шаг 6: Ветвление - либо email (при ошибке), либо сразу конец (при успехе)
    check_results >> log_error_email_task
    check_results >> end

    # Шаг 7: Если был email

    log_error_email_task >> end
