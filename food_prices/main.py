
"""
 ETL for food_prices.csv 
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('AIVEN_DB_HOST'),
    'port': os.getenv('AIVEN_DB_PORT'),
    'database': os.getenv('AIVEN_DB_NAME'),
    'user': os.getenv('AIVEN_DB_USER'),
    'password': os.getenv('AIVEN_DB_PASSWORD'),
    'sslmode': 'require'
}


# EXTRACT

print("kazi nzuri!! food_prices.csv read...")
df = pd.read_csv('food_prices.csv', encoding='utf-8')
print(f"Found {len(df)} records")


# TRANSFORM

print("msichana mwerevu!! coz Processing ina'happen...")


column_mapping = {
    'Admin 1': 'admin1',
    'Admin 2': 'admin2',
    'Market Name': 'market_name'
}
df.rename(columns=column_mapping, inplace=True)

# Clean remaining column names
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

# Convert price_date to datetime (assuming DD/MM/YYYY format)
df['price_date'] = pd.to_datetime(df['price_date'], dayfirst=True)

# Add comprehensive time dimensions
df['year'] = df['price_date'].dt.year
df['month'] = df['price_date'].dt.month
df['day'] = df['price_date'].dt.day
df['day_of_week'] = df['price_date'].dt.dayofweek
df['day_name'] = df['price_date'].dt.day_name()
df['week_of_year'] = df['price_date'].dt.isocalendar().week
df['quarter'] = df['price_date'].dt.quarter
df['is_weekend'] = df['price_date'].dt.dayofweek >= 5
df['month_year'] = df['price_date'].dt.to_period('M').astype(str)
df['year_month'] = df['price_date'].dt.strftime('%Y-%m')

# Convert price to numeric
df['price'] = pd.to_numeric(df['price'])

# Clean text fields 
text_cols = ['country', 'admin1', 'admin2', 'market_name', 'commodity']
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()
    else:
        print(f"Warning: Column '{col}' not found in DataFrame")


# LOAD

print("msichana genius!! coz its Connecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create table with consistent column names
cursor.execute("""
CREATE TABLE IF NOT EXISTS food_prices (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    admin1 VARCHAR(100),
    admin2 VARCHAR(100),
    market_name VARCHAR(100),
    commodity VARCHAR(100),
    price_type VARCHAR(50),
    price_date DATE,
    collection_frequency VARCHAR(50),
    price DECIMAL(10,2),
    unit VARCHAR(20),
    currency VARCHAR(10),
    data_source VARCHAR(100),
    trend VARCHAR(50),
    pewi VARCHAR(50),
    alps_phase VARCHAR(50),
    data_type VARCHAR(50),
    upper_95_ci DECIMAL(10,2),
    lower_95_ci DECIMAL(10,2),
    forecast_methodology VARCHAR(100),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    week_of_year INTEGER,
    quarter INTEGER,
    is_weekend BOOLEAN,
    month_year TEXT,
    year_month TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
conn.commit()

# Prepare data for insertion with verified columns
cols_to_load = [
    'country', 'admin1', 'admin2', 'market_name', 'commodity',
    'price_type', 'price_date', 'collection_frequency', 'price',
    'unit', 'currency', 'data_source', 'trend', 'pewi',
    'alps_phase', 'data_type', 'upper_95_ci', 'lower_95_ci',
    'forecast_methodology', 'year', 'month', 'day',
    'day_of_week', 'day_name', 'week_of_year', 'quarter',
    'is_weekend', 'month_year', 'year_month'
]

# Filter to only columns that exist in the DataFrame
cols_to_load = [col for col in cols_to_load if col in df.columns]

# Insert data in batches
batch_size = 1000
for i in range(0, len(df), batch_size):
    batch = df[cols_to_load].iloc[i:i+batch_size]
    rows = [tuple(row) for _, row in batch.iterrows()]
    columns = sql.SQL(',').join([sql.Identifier(col) for col in cols_to_load])
    values = sql.SQL(',').join([sql.Placeholder() for _ in cols_to_load])
    insert_sql = sql.SQL("INSERT INTO food_prices ({}) VALUES ({})").format(columns, values)
    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f"Inserted rows {i+1} to {i+len(batch)}")

cursor.close()
conn.close()
print("ETL process completed!..WOOOW!!!")
