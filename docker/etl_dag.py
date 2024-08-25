


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 24),
}

# Define the DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for extracting, transforming, and loading data',
    schedule_interval='@daily',
    catchup=False,
)

# Extraction Task
def extract_data():
    users_csv_path = "/opt/airflow/csv/usersIncr.csv"
    transactions_csv_path = "/opt/airflow/csv/transactionsIncr.csv"
    
    users_dtype_dict = {
        'userid': int,
        'name': str,
        'contact': str,
        'balance': float
    }

    transactions_dtype_dict = {
        'transactionid': int,
        'userid': int,
        'amount': float,
        'type': str,
        'status': str,
        'method': str,
    }

    users_df = pd.read_csv(users_csv_path, dtype=users_dtype_dict)
    transactions_df = pd.read_csv(transactions_csv_path, dtype=transactions_dtype_dict, parse_dates=['date'])
    
    # Save dataframes to a temporary location for subsequent tasks
    users_df.to_pickle('/tmp/users_df.pkl')
    transactions_df.to_pickle('/tmp/transactions_df.pkl')

# Transformation Tasks
def transform_users():
    users_df = pd.read_pickle('/tmp/users_df.pkl')
    
    users_df = users_df.drop_duplicates(subset=['userid'])
    users_df = users_df.fillna({
        'name': 'unknown',
        'contact': 'not provided',
        'balance': 0.0
    })
    users_df = users_df[users_df['contact'].str.match(r'^\d{10}$', na=False)]
    users_df.columns = [col.lower() for col in users_df.columns]
    
    users_df.to_pickle('/tmp/cleaned_users_df.pkl')

def transform_transactions():
    transactions_df = pd.read_pickle('/tmp/transactions_df.pkl')
    
    transactions_df = transactions_df.drop_duplicates(subset=['transactionid'])
    transactions_df = transactions_df[transactions_df['amount'] >= 0]
    transactions_df = transactions_df.fillna({
        'amount': 0.0,
        'type': 'unknown',
        'status': 'unknown',
        'method': 'not specified'
    })
    transactions_df['transactionid'] = transactions_df['transactionid'].astype(int)
    transactions_df['amount'] = pd.to_numeric(transactions_df['amount'], errors='coerce')
    transactions_df['date'] = pd.to_datetime(transactions_df['date'], errors='coerce')
    transactions_df.columns = [col.lower() for col in transactions_df.columns]
    transactions_df = transactions_df[transactions_df['amount'] <= 10000]
    
    transactions_df.to_pickle('/tmp/cleaned_transactions_df.pkl')

# Load Task
def load_data():
    cleaned_users_df = pd.read_pickle('/tmp/cleaned_users_df.pkl')
    cleaned_transactions_df = pd.read_pickle('/tmp/cleaned_transactions_df.pkl')

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')

    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')
    
    upsert_users_query = """
    INSERT INTO users (userid, name, contact, balance)
    VALUES (:userid, :name, :contact, :balance)
    ON CONFLICT (userid) DO UPDATE SET
        name = EXCLUDED.name,
        contact = EXCLUDED.contact,
        balance = EXCLUDED.balance;
    """

    transactions_upsert_stmt = text("""
    INSERT INTO transactions (transactionid, amount, type, date, status, method, userid)
    VALUES (:transactionid, :amount, :type, :date, :status, :method, :userid)
    ON CONFLICT (transactionid) DO UPDATE SET
        amount = EXCLUDED.amount,
        type = EXCLUDED.type,
        date = EXCLUDED.date,
        status = EXCLUDED.status,
        method = EXCLUDED.method,
        userid = EXCLUDED.userid;
    """)

    update_balance_query = """
    WITH transaction_summaries AS (
        SELECT
            userid,
            SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
            SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits
        FROM transactions
        WHERE status = 'completed'
          AND date > :last_update_date
        GROUP BY userid
    )
    UPDATE users u
    SET balance = u.balance +
                  COALESCE(ts.total_credits, 0) -
                  COALESCE(ts.total_debits, 0)
    FROM transaction_summaries ts
    WHERE u.userid = ts.userid;
    """

    fetch_last_update_date_query = "SELECT last_update_date FROM updatemetadata WHERE id = 1"
    update_last_update_date_query = "UPDATE updatemetadata SET last_update_date = :new_update_date WHERE id = :id"

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        with engine.connect() as connection:
            for user_data in cleaned_users_df.to_dict(orient='records'):
                connection.execute(text(upsert_users_query), user_data)

            for transaction_data in cleaned_transactions_df.to_dict(orient='records'):
                connection.execute(transactions_upsert_stmt, transaction_data)

            result = connection.execute(text(fetch_last_update_date_query))
            last_update_date = result.scalar()
            connection.execute(text(update_balance_query), {'last_update_date': last_update_date})

            new_update_date = datetime.now()
            connection.execute(text(update_last_update_date_query), {'new_update_date': new_update_date, 'id': 1})

            connection.commit()
    except Exception as e:
        print(f"Error during load operation: {e}")
    finally:
        session.close()

# Define Airflow tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_users_task = PythonOperator(
    task_id='transform_users',
    python_callable=transform_users,
    dag=dag,
)

transform_transactions_task = PythonOperator(
    task_id='transform_transactions',
    python_callable=transform_transactions,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)



extract_task >> [transform_users_task, transform_transactions_task]
[transform_users_task, transform_transactions_task] >> load_task
