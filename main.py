import requests
import pandas as pd
import psycopg2
from math import log2
from psycopg2 import sql
import os

# Database connection details
DB_HOST = "localhost"
DB_NAME = "gene_expression"
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Fetching gene data from Ensembl API
def fetch_ensembl_gene_data(gene_id):
    print(f"Fetching data for gene {gene_id}...")  # Added log for fetching data
    url = f'https://rest.ensembl.org/lookup/id/{gene_id}?content-type=application/json'
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Data fetched for {gene_id}.")
        return response.json()
    else:
        print(f"Error fetching data for {gene_id}: {response.status_code}")
        return None

# Normalize gene expression data
def normalize_expression(df):
    print("Normalizing gene expression data...")
    df['expression'] = df['expression'].apply(lambda x: log2(x + 1))  # Log2 normalization
    return df

# Get database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Load data into PostgreSQL
def load_to_postgresql(df):
    conn = get_db_connection()
    if conn is None:
        print("Failed to connect to the database. Exiting load process.")
        return

    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            insert_query = sql.SQL("""
                INSERT INTO gene_expression (gene_id, display_name, length, seq_region_name, start, "end", expression, biotype, description, canonical_transcript, strand, species)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (row['gene_id'], row['display_name'], row['length'], row['seq_region_name'], row['start'], row['end'], row['expression'], row['biotype'], row['description'], row['canonical_transcript'], row['strand'], row['species']))
            print(f"Inserted data for gene {row['gene_id']}")  # Log each insertion
        except Exception as e:
            print(f"Error inserting data for gene {row['gene_id']}: {e}")

    conn.commit()
    print("Data committed to the database.")
    cursor.close()
    conn.close()

# Main ETL process
def run_etl_pipeline():
    genes = ['ENSG00000139618', 'ENSG00000157764', 'ENSG00000181449', 'ENSG00000012048', 'ENSG00000141510']  # Example gene list (BRCA1, TP53)

    # Extract
    extracted_data = []  # List to store the extracted data
    for gene in genes:
        print(f"Fetching data for gene {gene}...")
        gene_data = fetch_ensembl_gene_data(gene)
        if gene_data:
            # Get the required data from the Ensembl API response
            expression_value = gene_data.get('length', 0)  # Default to 0 if 'length' is not available

            # Append the full set of data to extracted_data list
            extracted_data.append({
                'gene_id': gene_data['id'],
                'display_name': gene_data['display_name'],
                'length': gene_data.get('length', 0),
                'seq_region_name': gene_data.get('seq_region_name', ''),
                'start': gene_data.get('start', 0),
                'end': gene_data.get('end', 0),
                'expression': expression_value,
                'biotype': gene_data.get('biotype', ''),
                'description': gene_data.get('description', ''),
                'canonical_transcript': gene_data.get('canonical_transcript', ''),
                'strand': gene_data.get('strand', 0),
                'species': gene_data.get('species', '')
            })
            print(f"Data fetched for {gene} with expression {expression_value}")
        else:
            print(f"Failed to fetch data for {gene}.")

    if not extracted_data:
        print("No data extracted. Exiting ETL pipeline.")
        return

    # Transform: Convert the extracted data into a DataFrame and normalize
    df = pd.DataFrame(extracted_data)
    print("Data extracted. Normalizing data...")
    transformed_df = normalize_expression(df)

    # Load: Insert the transformed data into PostgreSQL
    print("Loading data to PostgreSQL...")
    load_to_postgresql(transformed_df)
    print("ETL pipeline completed successfully!")

# Run the pipeline
run_etl_pipeline()
