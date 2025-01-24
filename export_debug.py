import csv
import os
import sys
import json
from ethiopian_date import EthiopianDateConverter
import hashlib
import zipfile
import glob
import logging
import mysql.connector

# Configure logging
logging.basicConfig(
    filename='export_tool.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def resource_path(relative_path):
    """ Get the absolute path to the resource (works for development and PyInstaller) """
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        return os.path.join(os.path.abspath("."), relative_path)


def load_config(config_path):
    """Load JSON configuration file."""
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logging.error(f"Error: Config file not found at {config_path}")
        return {"queries_path": {}, "db_properties": {}}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON file: {e}")
        return {"queries_path": {}, "db_properties": {}}


def zip_files_with_checksum(folder_path, zip_name):
    """Creates a zip file of all files in folder_path and generates a SHA-256 checksum."""
    zip_path = os.path.join(folder_path, f"{zip_name}.zip")
    checksum_file = os.path.join(folder_path, f"{zip_name}_checksum.txt")
    
    # Step 1: Create zip file
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, arcname=os.path.relpath(file_path, folder_path))
    
    # Step 2: Generate SHA-256 checksum
    sha256_hash = hashlib.sha256()
    with open(resource_path(zip_path), "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    checksum = sha256_hash.hexdigest()
    
    # Step 3: Save checksum to file
    with open(resource_path(checksum_file), 'w') as f:
        f.write(checksum)
    
    logging.info(f"Zip file created at: {zip_path}")
    logging.info(f"Checksum saved to: {checksum_file}")


def read_sql_file(file_path):
    """ Read and return the content of a SQL file """
    try:
        with open(resource_path(file_path), 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        logging.error(f"SQL file {file_path} not found.")
        return None


def export_to_csv(db_config, queries, gregorian_start_date, gregorian_end_date, month, year):
    try:
        conn = mysql.connector.connect(
            host=db_config["DB_HOST"],
            user=db_config["DB_USER"],
            password=db_config["DB_PASS"],
            database=db_config["DB_NAME"],
            auth_plugin='mysql_native_password',
        )
        cursor = conn.cursor()
        
        if not os.path.exists('exported_data'):
            os.makedirs('exported_data')
        
        cursor.execute(facility_details_query)
        facility_details = cursor.fetchall()
        raw_facility_name = facility_details[0][2]
        raw_woreda = facility_details[0][1]
        raw_region = facility_details[0][0]
        facility_name = raw_facility_name.replace(" ", "").replace("_", "")
        cursor.execute(hmiscode_query)
        hmiscode = cursor.fetchall()
        hmiscode = hmiscode[0][0].replace(" ", "").replace("_", "")
        for query_name, query in queries.items():
            formatted_query = query.replace("REPORT_END_DATE", f"'{gregorian_end_date}'").replace(
                "REPORT_START_DATE", f"'{gregorian_start_date}'")
            cursor.execute(formatted_query)
            results = cursor.fetchall()
            
            modified_results = [row + (
                raw_region, raw_woreda, raw_facility_name, hmiscode)
                                for row in results]
            csv_file_path = os.path.join('exported_data',
                                         f"{query_name}_{facility_name}{hmiscode}_{month}_{year}.csv")
            if modified_results:
                with open(resource_path(csv_file_path), mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([i[0] for i in cursor.description] + additional_columns)
                    writer.writerows(modified_results)
            else:
                logging.warning(f"No data returned for {query_name}.")
        
        logging.info("Data exported to exported_data folder.")
        # ZIP generated files
        output_folder = "exported_data"
        zip_files_with_checksum(output_folder,
                                f"{facility_name}{hmiscode}_{month}_{year}")
        # Delete generated files
        file_pattern = os.path.join('exported_data',
                                    f"*{facility_name}{hmiscode}_{month}_{year}.csv")
        for file_path in glob.glob(file_pattern):
            try:
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")
            except OSError as e:
                logging.error(f"Error deleting file {file_path}: {e}")
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def run_queries_for_combinations(db_configs, year_month_combinations):
    """Run queries for a set of database and year/month combinations."""
    for db_config in db_configs:
        for year, month in year_month_combinations:
            month_index = month_mapping.get(month)
            conv = EthiopianDateConverter.to_gregorian
            
            gregorian_end_date = conv(year, month_index, 20)
            if month_index == 1:
                gregorian_start_date = conv(year - 1, 12, 21)
            else:
                gregorian_start_date = conv(year, month_index - 1, 21)
            
            queries = {}
            for tag, path in export_config['queries_path'].items():
                query = read_sql_file(resource_path(path))
                if query:
                    queries[tag] = query
            if queries:
                export_to_csv(db_config, queries, gregorian_start_date, gregorian_end_date, month,
                              year)
            else:
                logging.error("No valid queries found.")


# Constants
additional_columns = ['Region', 'Woreda', 'Facility', 'HMISCode']
months = ["Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir", "Yekatit", "Megabit", "Miyazia", "Ginbot",
          "Sene", "Hamle", "Nehase", "Puagume"]
month_mapping = {name: index + 1 for index, name in enumerate(months)}

facility_details_query = """
select state_province as Region, city_village as Woreda, mamba_dim_location.name as Facility from mamba_fact_location_tag
join mamba_fact_location_tag_map on mamba_fact_location_tag.location_tag_id=mamba_fact_location_tag_map.location_tag_id
join mamba_dim_location on mamba_dim_location.location_id = mamba_fact_location_tag_map.location_id where mamba_fact_location_tag.name='Facility Location';
"""
hmiscode_query = """
select value_reference as HMISCode from mamba_fact_location_attribute
join mamba_fact_location_attribute_type on mamba_fact_location_attribute.attribute_type_id=mamba_fact_location_attribute_type.location_attribute_type_id
where name='hmiscode';
"""

# Load configuration
export_config = load_config(resource_path("export_config.json"))

# Define database configurations
db_configs = [
    {"DB_HOST": "localhost", "DB_USER": "root", "DB_PASS": "Abcd@1234", "DB_NAME": "analytics_areket"},
    {"DB_HOST": "localhost", "DB_USER": "root", "DB_PASS": "Abcd@1234", "DB_NAME": "analytics_emdiber"},
    {"DB_HOST": "localhost", "DB_USER": "root", "DB_PASS": "Abcd@1234", "DB_NAME": "analytics_gunchrie"},
    {"DB_HOST": "localhost", "DB_USER": "root", "DB_PASS": "Abcd@1234", "DB_NAME": "analytics_lera"}
]

# Define year/month combinations
year_month_combinations = [
    (2016, "Meskerem"),
    (2016, "Tir"),
    (2015, "Meskerem"),
    (2015, "Tir"),
]

# Run queries for all combinations
run_queries_for_combinations(db_configs, year_month_combinations)