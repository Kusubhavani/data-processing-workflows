import os
import pandas as pd
import numpy as np
from faker import Faker

fake = Faker('en_US')

np.random.seed(42)
n_rows = 100
data = {
    'id': range(1, n_rows + 1),
    'name': [fake.name() for _ in range(n_rows)],
    'age': np.random.randint(18, 66, n_rows),
    'city': np.random.choice(
        ['New York', 'San Francisco', 'Chicago', 'Los Angeles',
         'Boston', 'Seattle', 'Houston', 'Miami', 'Phoenix', 'Denver'],
        n_rows
    ),
    'salary': np.round(np.random.uniform(40000, 100000, n_rows), 2),
    'joindate': [
        fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        for _ in range(n_rows)
    ],
}
df = pd.DataFrame(data)

os.makedirs('data', exist_ok=True)
df.to_csv('data/input.csv', index=False)
print(f"Generated data/input.csv with {n_rows} rows")
