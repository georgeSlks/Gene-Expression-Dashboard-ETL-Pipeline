import dash
import dash_table
import psycopg2
import pandas as pd
from dash import html
import os

# Setup Dash app
app = dash.Dash(__name__)

# Database connection details
DB_HOST = "localhost"
DB_NAME = "gene_expression"
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


# Get database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


# Fetch data from PostgreSQL
def fetch_data_from_postgres():
    conn = get_db_connection()
    if conn is None:
        return None

    # SQL query to select all data
    query = "SELECT * FROM gene_expression"

    # Use pandas to execute the query and retrieve data into a DataFrame
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        conn.close()


# Fetch the data from PostgreSQL
df = fetch_data_from_postgres()

# Ensure we got data before continuing
if df is not None:
    # Create a Dash table for the data
    app.layout = html.Div([
        html.H1("Gene Expression Data", style={'textAlign': 'center'}),
        dash_table.DataTable(
            id='gene-expression-table',
            columns=[{"name": col, "id": col} for col in df.columns],
            data=df.to_dict('records'),
            style_table={'height': '350px', 'overflowY': 'auto'},
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': 'rgb(210, 210, 210)', 'fontWeight': 'bold'},
            style_data={'backgroundColor': 'rgb(250, 250, 250)', 'whiteSpace': 'normal'}
        )
    ])

    # Run the Dash app
    if __name__ == '__main__':
        app.run_server(debug=True)
else:
    print("No data fetched from the database. Exiting.")
