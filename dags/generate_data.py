import os
from datetime import datetime
from collections import OrderedDict
from airflow import Dataset
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

from faker import Faker
import psycopg2

start_date_cons=datetime(2022, 11, 1)


@dag(start_date=start_date_cons, schedule_interval="@daily", catchup=True, max_active_runs=1)
def generate_data():
    hook = PostgresHook(postgres_conn_id="cosmic_finance")

    @task(outlets=Dataset('customers'), depends_on_past=True, wait_for_downstream=True)
    def create_customers(**kwargs):
        fake = Faker()
        conn = hook.get_conn()
        
        execution_date = kwargs["ds"]
        days_since_start = (datetime.strptime(execution_date, '%Y-%m-%d') - start_date_cons).days

        if days_since_start <= 3:
            customer_count = fake.pyint(min_value=50, max_value=100)
        elif days_since_start <= 21:
            customer_count = fake.pyint(min_value=30, max_value=50)
        elif days_since_start <= 90:
            customer_count = fake.pyint(min_value=20, max_value=30)
        else: 
            customer_count = fake.pyint(min_value=0, max_value=5)
            
        print(f"it has been {days_since_start} days since start date and {customer_count} customer records will be made")

        returned_keys = []

        for i in range(customer_count):
            cur = conn.cursor()

            insert_column_list = "first_name, last_name, city, mobile_no, pancard_no, dob, created_at"
            values_list = [f"'{fake.first_name()}'", f"'{fake.last_name()}'", f"'{fake.city()}'", 
                           f"'{fake.phone_number()}'", f"'{fake.iban()}'", f"'{fake.date_of_birth()}'", f"'{execution_date} {fake.time()}'"]

            values_string = ', '.join(map(str,values_list))
            
            cur.execute(f"""INSERT INTO customers ({insert_column_list})
                            VALUES ({values_string}) RETURNING customer_id""")
            conn.commit()
            returned_keys.append(cur.fetchone()[0])
            cur.close()
        
        conn.close()
        return returned_keys

    @task(outlets=Dataset('accounts'))
    def create_account(customer_ids: list, **kwargs):
        fake = Faker()
        conn = hook.get_conn()

        execution_date = kwargs["ds"]

        returned_keys = []

        for customer_id in customer_ids:
            cur = conn.cursor()

            insert_column_list = "customer_id, balance, account_status, account_type, currency, created_at"
            values_list = [customer_id, float(fake.pydecimal(right_digits=2, positive=True, max_value=100000)), 
                           "'Active'", "'Personal'", "'USD'", f"'{execution_date} {fake.time()}'"]

            values_string = ', '.join(map(str,values_list))

            cur.execute(f"""INSERT INTO accounts ({insert_column_list}) 
                            VALUES ({values_string}) RETURNING account_id""")
            conn.commit()
            returned_keys.append(cur.fetchone()[0])
            cur.close()
        
        conn.close()
        return returned_keys

    @task()
    def pick_transaction_customers(returned_keys: list, **kwargs):
        fake = Faker()
        conn = hook.get_conn()

        execution_date = kwargs["ds"]
        days_since_start = (datetime.strptime(execution_date, '%Y-%m-%d') - start_date_cons).days
        
        if days_since_start <= 3:
            customer_count = fake.pyint(min_value=50, max_value=100)
        elif days_since_start <= 21:
            customer_count = fake.pyint(min_value=30, max_value=50)
        elif days_since_start <= 90:
            customer_count = fake.pyint(min_value=20, max_value=30)
        else: 
            customer_count = fake.pyint(min_value=0, max_value=5)
            
        print(f"it has been {days_since_start} days since start date and {customer_count} customer records will be made")

        cur = conn.cursor()
        query =  f"""with random_account1 as
                            (select ROW_NUMBER() OVER (order by random()) join_key
                            , account_id 
                            from accounts),
                    random_account2 as
                            (select ROW_NUMBER() OVER (order by random()) join_key
                            , account_id 
                            from accounts)
                    select r1.account_id , r2.account_id
                    from random_account1 r1
                    join random_account2 r2 on r1.join_key = r2.join_key
                    where r1.join_key <= {customer_count}
                    ;"""

        cur.execute(query)
        return cur.fetchall()

    @task(outlets=Dataset('transactions'))
    def create_transactions(customer_ids: list, **kwargs):
        fake = Faker()
        conn = hook.get_conn()

        returned_keys = []
        execution_date = kwargs["ds"]


        for customer_id_row in customer_ids:
            cur = conn.cursor()

            insert_column_list = "transaction_type, from_account_id, to_account_id, date_issued, amount, transaction_medium, created_at"

            values_list_debit = ["'DEBIT'", customer_id_row[0], customer_id_row[1], f"'{execution_date}'", float(fake.pydecimal(right_digits=2, positive=True, max_value=1000)), 
                            fake.random_element(elements=OrderedDict([("'ACH'", 0.95), ("'SWIFT'", 0.05) ])), f"'{execution_date} {fake.time()}'"
                            ]
            values_string_debit = ', '.join(map(str,values_list_debit))

            print(f"debit values are {values_string_debit}")

            #Credit record should be same values but from and to customer fields should be swapped and transaction type should be CREDIT
            values_list_credit = ["'CREDIT'", customer_id_row[1], customer_id_row[0], values_list_debit[3], values_list_debit[4], values_list_debit[5], f"'{execution_date} {fake.time()}'"]
            values_string_credit = ', '.join(map(str,values_list_credit))

            print(f"credit values are {values_string_credit}")

            #DEBIT RECORDS
            cur.execute(f"""INSERT INTO transactions ({insert_column_list}) 
                        VALUES ({values_string_debit}) RETURNING transaction_id"""
            )
                            
            conn.commit()
            returned_keys.append(cur.fetchone()[0])
            cur.close()

            #CREDIT RECORDS
            cur = conn.cursor()
            cur.execute(f"""INSERT INTO transactions ({insert_column_list}) 
                        VALUES ({values_string_credit}) RETURNING transaction_id"""
            )
                            
            conn.commit()
            returned_keys.append(cur.fetchone()[0])
            cur.close()
        
        conn.close()
        return returned_keys

    @task(outlets=Dataset('loans'))
    def create_loans(returned_keys: list, **kwargs):
        fake = Faker()
        conn = hook.get_conn()

        returned_keys = []

        execution_date = kwargs["ds"]
        days_since_start = (datetime.strptime(execution_date, '%Y-%m-%d') - start_date_cons).days

        if days_since_start <= 3:
            customer_count = fake.pyint(min_value=0, max_value=20)
        elif days_since_start <= 21:
            customer_count = fake.pyint(min_value=0, max_value=8)
        elif days_since_start <= 90:
            customer_count = fake.pyint(min_value=0, max_value=4)
        else: 
            customer_count = fake.pyint(min_value=0, max_value=3)
            
        print(f"it has been {days_since_start} days since start date and {customer_count} customer records will be made")

        cur = conn.cursor()
        cur.execute(f"""select customer_id from customers order by random() limit {customer_count}"""
        )
        customer_id_list=cur.fetchall()

        for customer_id in customer_id_list:
            cur = conn.cursor()

            insert_column_list = "customer_id, loan_amount, date_issued, created_at"

            values_list_loan = [customer_id[0], float(fake.pydecimal(right_digits=2, positive=True, max_value=1000)), f"'{execution_date}'", f"'{execution_date} {fake.time()}'"]
            values_string_loan = ', '.join(map(str,values_list_loan))

            print(f"loan values are {values_string_loan}")

            #LOAN RECORDS
            cur.execute(f"""INSERT INTO loans ({insert_column_list}) 
                        VALUES ({values_string_loan}) RETURNING loan_id"""
            )
                            
            conn.commit()
            returned_keys.append(cur.fetchone()[0])
            cur.close()
        
        conn.close()
        return returned_keys

    @task()
    def outbound_feeds_map(returned_keys_dummy1: list, returned_keys_dummy2: list, **kwargs):
        conn = hook.get_conn()

        execution_date = kwargs["ds"]

        cur = conn.cursor()

        cur.execute(f"""select table_name from information_schema.tables where table_schema = 'public';"""
        )
        table_list=cur.fetchall()

        cur.close()
        conn.close()

        return table_list

    @task()
    def write_outbound_feed(table_name: list, **kwargs):
        conn = hook.get_conn()

        execution_date = kwargs["ds"]

        path = '/tmp/data'
        try:
            os.mkdir(path)
        except OSError as error:
            print(error)  
        cur = conn.cursor()

        with open(f'{path}/{table_name[0]}-{execution_date}.csv', 'w') as f:
            cur.copy_expert(f"""COPY (select 'C' as CDC_TYPE, * from {table_name[0]} where date(created_at) = '{execution_date}') 
                                TO STDOUT WITH CSV HEADER DELIMITER '|'""", f)

        cur.close()
        conn.close()

        return True



    create_customers_group = create_account(create_customers())
    create_transactions_group = create_transactions(pick_transaction_customers(create_customers_group))
    create_loans_group = create_loans(create_customers_group)
    write_outbound_feeds_group = write_outbound_feed.expand(table_name=outbound_feeds_map(create_loans_group, create_transactions_group))

    

generate_data_dag = generate_data()