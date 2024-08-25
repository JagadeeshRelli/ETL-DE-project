# # from airflow import DAG
# # from airflow.operators.python_operator import PythonOperator
# # from airflow.utils.dates import days_ago


# # # Retrieve credentials from environment variables
# # db_user = os.getenv('DB_USER')
# # db_password = os.getenv('DB_PASSWORD')
# # db_host = os.getenv('DB_HOST')
# # db_name = os.getenv('DB_NAME')

# # # Create an SQLAlchemy engine
# # engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')

# # # Define SQL queries
# # upsert_users_query = """
# # INSERT INTO users (userid, name, contact, balance)
# # VALUES (:userid, :name, :contact, :balance)
# # ON CONFLICT (userid)
# # DO UPDATE SET
# #     name = EXCLUDED.name,
# #     contact = EXCLUDED.contact,
# #     balance = EXCLUDED.balance;
# # """

# # transactions_upsert_stmt = text("""
# # INSERT INTO Transactions (transactionid, amount, type, date, status, method, userid)
# # VALUES (:transactionid, :amount, :type, :date, :status, :method, :userid)
# # ON CONFLICT (transactionid) DO UPDATE SET
# # amount = EXCLUDED.amount,
# # type = EXCLUDED.type,
# # date = EXCLUDED.date,
# # status = EXCLUDED.status,
# # method = EXCLUDED.method,
# # userid = EXCLUDED.userid;
# # """)

# # update_balance_query = """
# # WITH transaction_summaries AS (
# #     SELECT
# #         userid,
# #         SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
# #         SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits
# #     FROM
# #         transactions
# #     WHERE
# #         status = 'completed'
# #         AND date > :last_update_date 
# #     GROUP BY
# #         userid
# # )
# # UPDATE users u
# # SET balance = u.balance +
# #               COALESCE(ts.total_credits, 0) -
# #               COALESCE(ts.total_debits, 0)
# # FROM transaction_summaries ts
# # WHERE u.userid = ts.userid;

# # """

# # fetch_last_update_date_query = "SELECT last_update_date FROM updatemetadata where id=1"
# # update_last_update_date_query = "UPDATE updatemetadata SET last_update_date = :new_update_date WHERE id = :id"

# # def upsert_users_transactions(**kwargs):
# #     users_csv = kwargs['users_csv']
# #     transactions_csv = kwargs['transactions_csv']

# #     # Define the data types for the users CSV columns
# #     users_dtype_dict = {
# #         'userid': int,      
# #         'name': str,        
# #         'contact': str,     
# #         'balance': float    
# #     }

# #     # Define the data types for the transactions CSV columns
# #     transactions_dtype_dict = {
# #         'transactionid': int,
# #         'userid': int,
# #         'amount': float,
# #         'type': str,
# #         'status': str,
# #         'method': str,
# #         # Uncomment the following line if the 'date' column needs to be parsed as a datetime
# #         #'date': 'datetime64' 
# #     }

# #     # Load CSV data into pandas DataFrame
# #     df_users_incr = pd.read_csv(users_csv, dtype=users_dtype_dict)
# #     df_transactions_incr = pd.read_csv(transactions_csv, dtype=transactions_dtype_dict,parse_dates=['date'])

# #     # Convert the DataFrames to lists of dictionaries
# #     new_users = df_users_incr.to_dict(orient='records')
# #     new_transactions = df_transactions_incr.to_dict(orient='records')

# #     try:
# #         with engine.connect() as connection:
# #             for user_data in new_users:
# #                 connection.execute(text(upsert_users_query), user_data)
# #                 connection.commit()

# #             for transaction_data in new_transactions:
# #                 connection.execute(transactions_upsert_stmt, transaction_data)
# #                 connection.commit()

# #             result = connection.execute(text(fetch_last_update_date_query))
# #             last_update_date = result.scalar()
# #             connection.execute(text(update_balance_query), {'last_update_date': last_update_date})

# #             new_update_date = datetime.now()
# #             connection.execute(text(update_last_update_date_query), {'new_update_date': new_update_date, 'id': 1})

# #             connection.commit()

# #     except Exception as e:
# #         print(f"An error occurred: {e}")









# # pip install pandas
# # pip install SQLAlchemy
# # pip install psycopg2-binary


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago



# import pandas as pd
# import os
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker
# from datetime import datetime

# def clean_users(df):
#     df = df.drop_duplicates(subset=['userid'])  # Remove duplicates
#     df = df.fillna({
#         'name': 'unknown',         # Fill missing names
#         'contact': 'not provided', # Fill missing contacts
#         'balance': 0.0             # Fill missing balances
#     })
#     df = df[df['contact'].str.match(r'^\d{10}$', na=False)]  # Validate contact format
#     df.columns = [col.lower() for col in df.columns]  # Normalize column names
#     return df

# def clean_transactions(df):
#     df = df.drop_duplicates(subset=['transactionid'])  # Remove duplicates
#     df = df[df['amount'] >= 0]  # Filter out rows with negative amounts
#     df = df.fillna({
#         'amount': 0.0,              # Fill missing amounts
#         'type': 'unknown',          # Fill missing types
#         'status': 'unknown',        # Fill missing statuses
#         'method': 'not specified'   # Fill missing methods
#     })
#     df['transactionid'] = df['transactionid'].astype(int)  # Ensure 'transactionid' is integer
#     df['amount'] = pd.to_numeric(df['amount'], errors='coerce')  # Ensure 'amount' is float
#     df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Convert 'date' to datetime
#     df.columns = [col.lower() for col in df.columns]  # Normalize column names
#     df = df[df['amount'] <= 10000]  # Remove rows with outlier amounts
#     return df

# def upsert_users_transactions(**kwargs):
#     users_csv = kwargs['users_csv']
#     transactions_csv = kwargs['transactions_csv']



#     users_dtype_dict = {
#         'userid': int,
#         'name': str,
#         'contact': str,
#         'balance': float
#     }

#     transactions_dtype_dict = {
#         'transactionid': int,
#         'userid': int,
#         'amount': float,
#         'type': str,
#         'status': str,
#         'method': str,
#         # 'date': 'datetime64'
#     }

#     # Load the initial sample data
#     usersdf1 = pd.read_csv(
#         users_csv,
#         dtype=users_dtype_dict
#     )
#     print("\n\n\nusers data extracted\n")
#     usersdf1.info()

#     transactionsdf1 = pd.read_csv(
#         transactions_csv,
#         dtype=transactions_dtype_dict,
#         parse_dates=['date']
#     )
#     print("\n\ntransactions data extracted\n")
#     transactionsdf1.info()

#     # Clean the data
#     cleaned_users_df1 = clean_users(usersdf1)
#     cleaned_transactions_df1 = clean_transactions(transactionsdf1)

#     print("\nextracted users data and transactions data cleaned ")
#     # SQL queries for upsert and update operations
#     upsert_users_query = """
#     INSERT INTO users (userid, name, contact, balance)
#     VALUES (:userid, :name, :contact, :balance)
#     ON CONFLICT (userid) DO UPDATE SET
#         name = EXCLUDED.name,
#         contact = EXCLUDED.contact,
#         balance = EXCLUDED.balance;
#     """

#     transactions_upsert_stmt = text("""
#     INSERT INTO transactions (transactionid, amount, type, date, status, method, userid)
#     VALUES (:transactionid, :amount, :type, :date, :status, :method, :userid)
#     ON CONFLICT (transactionid) DO UPDATE SET
#         amount = EXCLUDED.amount,
#         type = EXCLUDED.type,
#         date = EXCLUDED.date,
#         status = EXCLUDED.status,
#         method = EXCLUDED.method,
#         userid = EXCLUDED.userid;
#     """)

#     update_balance_query = """
#     WITH transaction_summaries AS (
#         SELECT
#             userid,
#             SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END) AS total_credits,
#             SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END) AS total_debits
#         FROM transactions
#         WHERE status = 'completed'
#           AND date > :last_update_date
#         GROUP BY userid
#     )
#     UPDATE users u
#     SET balance = u.balance +
#                   COALESCE(ts.total_credits, 0) -
#                   COALESCE(ts.total_debits, 0)
#     FROM transaction_summaries ts
#     WHERE u.userid = ts.userid;
#     """

#     fetch_last_update_date_query = "SELECT last_update_date FROM updatemetadata WHERE id = 1"
#     update_last_update_date_query = "UPDATE updatemetadata SET last_update_date = :new_update_date WHERE id = :id"

#     # Retrieve credentials from environment variables
#     db_user = os.getenv('DB_USER')
#     db_password = os.getenv('DB_PASSWORD')
#     db_host = os.getenv('DB_HOST')
#     db_name = os.getenv('DB_NAME')

#     # Create an SQLAlchemy engine
#     engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}')

#     print("\nSQLAlchemy engine created")

#     # Create a session
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     # Perform database operations
#     try:
#         with engine.connect() as connection:
#             for user_data in cleaned_users_df1.to_dict(orient='records'):
#                 connection.execute(text(upsert_users_query), user_data)
#             print("\nusers data loaded into walletdb database")
            
#             for transaction_data in cleaned_transactions_df1.to_dict(orient='records'):
#                 connection.execute(transactions_upsert_stmt, transaction_data)
#             print("\ntransactions data loaded into walletdb database")

            
#             result = connection.execute(text(fetch_last_update_date_query))
#             last_update_date = result.scalar()
#             print("\nlast_update_date fetched from updatemetadata table",last_update_date)
            
#             connection.execute(text(update_balance_query), {'last_update_date': last_update_date})
#             print("\nusers balance updated")

            
#             new_update_date = datetime.now()
#             connection.execute(text(update_last_update_date_query), {'new_update_date': new_update_date, 'id': 1})
#             print("\nlast_update_date updated to ",new_update_date)
            
#             connection.commit()

#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         session.close()










# # Define default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'retries': 1,
# }

# # Define the DAG
# with DAG(
#     'upsert_users_transactions_dag',
#     default_args=default_args,
#     description='A DAG to upsert users and transactions from CSV into PostgreSQL',
#     schedule_interval='00 06 * * *',  # CRON expression for 6:00 AM IST
#     catchup=False,
 
# ) as dag:

#     upsert_task = PythonOperator(
#         task_id='upsert_users_transactions_task',
#         python_callable=upsert_users_transactions,
#         op_kwargs={
# #             df_users_incr = pd.read_csv('C:/Users/jagad/OneDrive/Desktop/paynearby/usersIncr.csv',dtype=users_dtype_dict)
# # df_transactions_incr = pd.read_csv('C:/Users/jagad/OneDrive/Desktop/paynearby/transactionsIncr.csv',dtype=transactions_dtype_dict)

#             'users_csv': '/opt/airflow/csv/usersIncr.csv',  # Adjust the path for the container
#             'transactions_csv': '/opt/airflow/csv/transactionsIncr.csv'  # Adjust the path for the container
#         }
#     )

#     upsert_task



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

# Set task dependencies
extract_task >> [transform_users_task, transform_transactions_task]
transform_users_task >> load_task
transform_transactions_task >> load_task



extract_task >> [transform_users_task, transform_transactions_task]
[transform_users_task, transform_transactions_task] >> load_task
