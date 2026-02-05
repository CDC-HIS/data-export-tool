import mysql.connector
import json
import os

def get_schema_info():
    try:
        # Load config
        with open('export_config.json', 'r') as f:
            config = json.load(f)
        
        db_props = config['db_properties']
        
        # Connect to database
        conn = mysql.connector.connect(
            host=db_props['DB_HOST'],
            user=db_props['DB_USER'],
            password=db_props['DB_PASS'],
            database=db_props['DB_NAME']
        )
        
        cursor = conn.cursor()
        
        # Get tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        print("Tables in database:")
        for table in tables:
            table_name = table[0]
            print(f"\nTable: {table_name}")
            
            # Get columns for each table
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[0]} ({col[1]})")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    get_schema_info()