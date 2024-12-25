#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
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
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def resource_path(relative_path):
    """ Get the absolute path to the resource (works for development and PyInstaller) """
    # print('Absolute path to home ',os.path.expanduser("~/Documents"))
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


export_config = load_config(resource_path("export_config.json"))

# MySQL credentials
DB_HOST = export_config["db_properties"]['DB_HOST']
DB_USER = export_config["db_properties"]['DB_USER']
DB_PASS = export_config["db_properties"]['DB_PASS']
DB_NAME = export_config["db_properties"]['DB_NAME']

start_year = 2013  # start year
end_year = 2022  # end year
years = [str(year) for year in range(start_year, end_year + 1)]
additional_columns = ['Region', 'Woreda', 'Facility', 'HMISCode']
months = ["Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir", "Yekatit", "Megabit", "Miyazia", "Ginbot",
          "Sene", "Hamle", "Nehase", "Puagume"]
month_mapping = {name: index + 1 for index, name in enumerate(months)}

root = tk.Tk()
root.title("SQL Extraction Tool")
root.geometry("400x200")
root.eval('tk::PlaceWindow . center')

progress = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
progress.grid_forget()

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
        messagebox.showerror("Error", f"SQL file {file_path} not found.")
        logging.error("Error", f"SQL file {file_path} not found.")
        return None


def export_to_csv(queries, gregorian_start_date, gregorian_end_date):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            auth_plugin='mysql_native_password',
        )
        cursor = conn.cursor()
        
        if not os.path.exists('exported_data'):
            os.makedirs('exported_data')
        
        total_queries = len(queries)
        progress['maximum'] = total_queries
        progress['value'] = 0
        progress.grid(row=6, column=0, columnspan=2, pady=10)
        cursor.execute(facility_details_query)
        facility_details = cursor.fetchall()
        raw_facility_name = facility_details[0][2]
        raw_woreda = facility_details[0][1]
        raw_region = facility_details[0][0]
        facility_name = raw_facility_name.replace(" ", "").replace("_", "")
        cursor.execute(hmiscode_query)
        hmiscode = cursor.fetchall()
        hmiscode = hmiscode[0][0].replace(" ", "").replace("_", "")
        for idx, (query_name, query) in enumerate(queries.items(), start=1):
            formatted_query = query.replace("REPORT_END_DATE", f"'{gregorian_end_date}'").replace(
                "REPORT_START_DATE", f"'{gregorian_start_date}'")
            cursor.execute(formatted_query)
            results = cursor.fetchall()
            
            modified_results = [row + (
                raw_region, raw_woreda, raw_facility_name, hmiscode)
                                for row in results]
            csv_file_path = os.path.join('exported_data',
                                         f"{query_name}_{facility_name}{hmiscode}_{combo_month.get()}_{entry_year.get()}.csv")
            if modified_results:
                with open(resource_path(csv_file_path), mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([i[0] for i in cursor.description] + additional_columns)
                    writer.writerows(modified_results)
            else:
                messagebox.showwarning("Warning", f"No data returned for {query_name}.")
                logging.warning(f"No data returned for {query_name}.")
            
            # Update the progress bar
            progress['value'] = idx
            root.update_idletasks()
        
        messagebox.showinfo("Success", "Data exported to exported_data folder.")
        logging.info("Data exported to exported_data folder.")
        # ZIP generated files
        output_folder = "exported_data"
        zip_files_with_checksum(output_folder,
                                f"{facility_name}{hmiscode}_{combo_month.get()}_{entry_year.get()}")
        # Delete generated files
        file_pattern = os.path.join('exported_data',
                                    f"*{facility_name}{hmiscode}_{combo_month.get()}_{entry_year.get()}.csv")
        for file_path in glob.glob(file_pattern):
            try:
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")
            except OSError as e:
                logging.error(f"Error deleting file {file_path}: {e}")
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error: {err}")
        logging.error(f"Error: {err}")
    finally:
        
        progress['value'] = 0  # Reset progress bar after completion
        progress.grid_forget()


def run_query():
    selected_month = combo_month.get()
    selected_year = entry_year.get()
    
    month = month_mapping.get(selected_month)
    year = int(selected_year)
    conv = EthiopianDateConverter.to_gregorian
    
    gregorian_end_date = conv(year, month, 20)
    if month == 1:
        gregorian_start_date = conv(year - 1, 12, 21)
    else:
        gregorian_start_date = conv(year, month - 1, 21)
    
    queries = {}
    for tag, path in export_config['queries_path'].items():
        query = read_sql_file(resource_path(path))
        if query:
            queries[tag] = query
    if queries:
        export_to_csv(queries, gregorian_start_date, gregorian_end_date)
    else:
        messagebox.showerror("Error", "No valid queries found.")
        logging.error("No valid queries found.")


# UI Components Month
tk.Label(root, text="Select Month:").grid(row=3, column=0, pady=5, padx=10, sticky="e")
combo_month = ttk.Combobox(root, values=months, state="readonly", width=25)
combo_month.grid(row=3, column=1, padx=10, pady=5, sticky="w")
combo_month.set(months[0])

# UI Components Year
tk.Label(root, text="Year (YYYY):").grid(row=4, column=0, pady=5, padx=10, sticky="e")
entry_year = ttk.Combobox(root, values=years, state="readonly", width=25)
entry_year.grid(row=4, column=1, padx=10, pady=5, sticky="w")
entry_year.set(years[4])  # Optionally set a default year

# UI Components Button
run_button = tk.Button(root, text="Run Query", command=run_query)
run_button.grid(row=5, column=0, columnspan=2, pady=10)
run_button.config(width=20)

for i in range(3, 6):
    root.grid_columnconfigure(0, weight=1)
    root.grid_columnconfigure(1, weight=1)
    root.grid_rowconfigure(i, weight=1)

root.mainloop()
