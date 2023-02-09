from datetime import datetime
from faker import Faker

#determine number of days since start_date
def num_days_since_start(kwargs["ds"], start_date_cons):
    days_since_start = (datetime.strptime(execution_date, '%Y-%m-%d') - start_date_cons).days
    return days_since_start

#return random number of days based on days from start_date
def random_days(days_since_start):
    if days_since_start <= 3:
        customer_count = fake.pyint(min_value=50, max_value=100)
    elif days_since_start <= 21:
        customer_count = fake.pyint(min_value=30, max_value=50)
    elif days_since_start <= 90:
        customer_count = fake.pyint(min_value=20, max_value=30)
    else: 
        customer_count = fake.pyint(min_value=0, max_value=5)
    return customer_count