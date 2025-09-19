#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
import csv
import os
import sys
import json
from ethiopian_date_converter.ethiopian_date_convertor import to_ethiopian, to_gregorian, EthDate
import hashlib
import zipfile
import glob
import logging
import mysql.connector
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='export_tool.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def resource_path(relative_path):

    if hasattr(sys, 'frozen'):
        if hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
            logging.info(f"Detected PyInstaller environment. Base path for resources: {base_path}")
        else:
            base_path = os.path.dirname(os.path.abspath(sys.argv[0]))
            logging.info(f"Detected Nuitka --onefile environment. Base path for resources: {base_path}")
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
        logging.info(f"Detected development environment. Base path for resources: {base_path}")

    full_path = os.path.join(base_path, relative_path)
    logging.debug(f"Resolved resource path for '{relative_path}': {full_path}")
    return full_path


def load_config(config_file_name="export_config.json"):

    external_config_path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), config_file_name)
    logging.info(f"Attempting to load config from external path: {external_config_path}")
    try:
        with open(external_config_path, 'r') as config_file:
            config = json.load(config_file)
            logging.info("External config file loaded successfully.")
            return config
    except FileNotFoundError:
        logging.warning(f"External config file not found at {external_config_path}. Looking for bundled default.")

        # If not found externally, try to load a bundled default config
        bundled_config_path = resource_path(config_file_name)
        logging.info(f"Attempting to load config from bundled path: {bundled_config_path}")
        try:
            with open(bundled_config_path, 'r') as config_file:
                config = json.load(config_file)
                logging.info("Bundled default config file loaded successfully.")
                return config
        except FileNotFoundError:
            logging.error(f"Error: Bundled default config file also not found at {bundled_config_path}")
            messagebox.showerror(
                "Config Not Found",
                f"Neither external nor bundled '{config_file_name}' found. Cannot proceed."
            )
            return {"queries_path": {}, "db_properties": {}}
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing bundled JSON file at {bundled_config_path}: {e}")
            messagebox.showerror("Config Error", f"Error parsing bundled '{config_file_name}': {e}\n"
                                                 "Bundled file is corrupt. Please report this issue.")
            return {"queries_path": {}, "db_properties": {}}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing external JSON file at {external_config_path}: {e}")
        messagebox.showerror("Config Error", f"Error parsing external '{config_file_name}': {e}\n"
                                             "Please check the file for syntax errors or delete it to use bundled default.")
        return {"queries_path": {}, "db_properties": {}}


# --- Global Configuration and UI Setup ---
#Prioritize external export config if not use packaged export_config
export_config = load_config("export_config.json")

db_properties = export_config.get("db_properties", {})
DB_HOST = db_properties.get('DB_HOST', 'localhost')
DB_USER = db_properties.get('DB_USER', 'openmrs')
DB_PASS = db_properties.get('DB_PASS', '')
DB_NAME = db_properties.get('DB_NAME', 'analytics_db')

# SQL queries mapping from config
QUERY_FILES = export_config.get("queries_path", {})

start_year = 2013
end_year = 2022
years = [str(year) for year in range(start_year, end_year + 1)]
additional_columns = ['Region', 'Woreda', 'Facility', 'HMISCode']
months = ["Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir", "Yekatit", "Megabit", "Miazia", "Ginbot",
          "Sene", "Hamle", "Nehassie", "Puagume"]
month_mapping = {name: index + 1 for index, name in enumerate(months)}

root = tk.Tk()
root.title("Data Extraction Tool")
root.geometry("400x200")
root.eval('tk::PlaceWindow . center')


style = ttk.Style()
style.theme_use('alt')
style.configure('TButton', font=('Arial', 10), padding=6)
style.configure('TLabel', font=('Arial', 10))
style.configure('TCombobox', font=('Arial', 10), padding=2)


progress = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
progress.grid(row=6, column=0, columnspan=2, pady=10, sticky="ew", padx=20)
progress.grid_remove()

facility_details_query_content = """
                                 SELECT state_province          AS Region, \
                                        city_village            AS Woreda, \
                                        mamba_dim_location.name AS Facility
                                 FROM mamba_fact_location_tag
                                          JOIN mamba_fact_location_tag_map ON mamba_fact_location_tag.location_tag_id = \
                                                                              mamba_fact_location_tag_map.location_tag_id
                                          JOIN mamba_dim_location ON mamba_dim_location.location_id = \
                                                                     mamba_fact_location_tag_map.location_id
                                 WHERE mamba_fact_location_tag.name = 'Facility Location'; \
                                 """
hmiscode_query_content = """
                         SELECT value_reference AS HMISCode
                         FROM mamba_fact_location_attribute
                                  JOIN mamba_fact_location_attribute_type \
                                       ON mamba_fact_location_attribute.attribute_type_id = \
                                          mamba_fact_location_attribute_type.location_attribute_type_id
                         WHERE name = 'hmiscode'; \
                         """



def zip_files_with_checksum(folder_path, zip_name):

    csv_archive_name = f"{zip_name}.zip"
    csv_archive_path = os.path.join(folder_path, csv_archive_name)

    checksum_name = f"{zip_name}_checksum.txt"
    checksum_file_path = os.path.join(folder_path, checksum_name)

    final_zip_path = os.path.join(folder_path, f"{zip_name}_packaged.zip")

    try:
        logging.info(f"Step 1: Creating temporary CSV archive at: {csv_archive_path}")
        with zipfile.ZipFile(csv_archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root_dir, _, files in os.walk(folder_path):
                for file in files:
                    if file.endswith(".csv"):
                        file_path = os.path.join(root_dir, file)
                        arcname = os.path.relpath(file_path, folder_path)
                        zipf.write(file_path, arcname=arcname)
        logging.info(f"Temporary CSV archive successfully created: {csv_archive_path}")

        logging.info(f"Step 2: Generating SHA-256 checksum for {zip_name}")
        sha256_hash = hashlib.sha256()
        with open(csv_archive_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        checksum_value = sha256_hash.hexdigest()
        logging.info(f"Checksum generated: {checksum_value}")

        logging.info(f"Step 3: Saving checksum to temporary file: {checksum_file_path}")
        with open(checksum_file_path, 'w') as f:
            f.write(checksum_value)
        logging.info(f"Checksum successfully saved to: {checksum_file_path}")

        logging.info(f"Step 4: Creating final zip file: {final_zip_path}")
        with zipfile.ZipFile(final_zip_path, 'w', zipfile.ZIP_DEFLATED) as final_zipf:
            final_zipf.write(csv_archive_path, arcname=f"{csv_archive_name}")
            logging.info(f"Added '{os.path.basename(csv_archive_path)}' to '{final_zip_path}'")

            final_zipf.write(checksum_file_path, arcname=f"{checksum_name}")
            logging.info(f"Added '{os.path.basename(checksum_file_path)}' to '{final_zip_path}'")

        logging.info(f"Final zip file created successfully at: {final_zip_path}")

        message_content = (
            "Operation Complete!\n\n"
            f"A final zip file has been created:\n"
            f"  '{final_zip_path}'\n\n"
            f"Containing zip file and checksum\n"
        )
        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo("Zip & Checksum Operation", message_content)

    except Exception as e:
        logging.error(f"An error occurred during zip and checksum creation: {e}", exc_info=True)
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Error", f"An error occurred during zip and checksum creation: {e}")
    finally:
        if os.path.exists(csv_archive_path):
            os.remove(csv_archive_path)
            logging.info(f"Cleaned up temporary file: {csv_archive_path}")
        if os.path.exists(checksum_file_path):
            os.remove(checksum_file_path)
            logging.info(f"Cleaned up temporary file: {checksum_file_path}")


def read_sql_file_content(file_path_relative_to_resources):
    full_path = resource_path(file_path_relative_to_resources)
    logging.info(f"Attempting to read SQL file: {full_path}")
    try:
        with open(full_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        logging.error(f"SQL file not found: {full_path}")
        messagebox.showerror("File Error", f"SQL file not found:\n{file_path_relative_to_resources}.")
        return None
    except Exception as e:
        logging.error(f"Error reading SQL file {full_path}: {e}")
        messagebox.showerror("File Error", f"Error reading SQL file:\n{file_path_relative_to_resources}\n{e}")
        return None


def export_to_csv(queries_to_execute, gregorian_start_date, gregorian_end_date):
    try:
        logging.info("Attempting to connect to MySQL database...")
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
        )
        cursor = conn.cursor()
        logging.info("Successfully connected to MySQL.")

        output_folder = 'exported_data'

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info(f"Created output directory: {output_folder}")

        total_queries = len(queries_to_execute)
        if total_queries == 0:
            messagebox.showwarning("No Queries", "No queries to execute based on configuration.")
            logging.warning("No queries to execute.")
            return

        progress['maximum'] = total_queries
        progress['value'] = 0
        progress.grid()

        logging.info("Executing facility details query...")
        cursor.execute(facility_details_query_content)
        facility_details = cursor.fetchall()

        if not facility_details:
            messagebox.showwarning("Warning", "No facility details found. Cannot proceed with export.")
            logging.warning("No facility details found from database.")
            return

        raw_facility_name = facility_details[0][2]
        raw_woreda = facility_details[0][1]
        raw_region = facility_details[0][0]
        facility_name_sanitized = raw_facility_name.replace(" ", "").replace("_", "")

        logging.info("Executing HMIS code query...")
        cursor.execute(hmiscode_query_content)
        hmiscode_result = cursor.fetchall()
        if not hmiscode_result:
            messagebox.showwarning("Warning", "No HMIS code found. Cannot proceed with export.")
            logging.warning("No HMIS code found from database.")
            return

        hmiscode_sanitized = hmiscode_result[0][0].replace(" ", "").replace("_", "")

        for idx, (query_name, query_content) in enumerate(queries_to_execute.items(), start=1):
            logging.info(f"Processing query: {query_name}")
            formatted_query = query_content.replace("REPORT_END_DATE", f"'{gregorian_end_date}'").replace(
                "REPORT_START_DATE", f"'{gregorian_start_date}'")

            try:
                cursor.execute(formatted_query)
                results = cursor.fetchall()
            except mysql.connector.Error as query_err:
                logging.error(f"Error executing query '{query_name}': {query_err}")
                messagebox.showerror("Query Error", f"Error executing query '{query_name}':\n{query_err}")
                continue

            modified_results = [row + (
                raw_region, raw_woreda, raw_facility_name, hmiscode_sanitized)
                                for row in results]

            csv_file_name = f"{query_name}_{facility_name_sanitized}{hmiscode_sanitized}_{combo_month.get()}_{entry_year.get()}.csv"
            csv_file_full_path = os.path.join(output_folder, csv_file_name)

            if modified_results:
                with open(csv_file_full_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([i[0] for i in cursor.description] + additional_columns)
                    writer.writerows(modified_results)
                logging.info(f"Data written to: {csv_file_full_path}")
            else:
                logging.warning(f"No data returned for {query_name}. Skipping CSV creation.")

            progress['value'] = idx
            root.update_idletasks()

        logging.info("All queries processed. Starting zip and checksum.")
        zip_files_with_checksum(output_folder,
                                f"{facility_name_sanitized}{hmiscode_sanitized}_{combo_month.get()}_{entry_year.get()}")

        # Delete generated CSV files after zipping
        file_pattern = os.path.join(output_folder,
                                    f"*{facility_name_sanitized}{hmiscode_sanitized}_{combo_month.get()}_{entry_year.get()}.csv")
        logging.info(f"Attempting to delete CSV files matching pattern: {file_pattern}")
        for file_path_to_delete in glob.glob(file_pattern):
            try:
                os.remove(file_path_to_delete)
                logging.info(f"Deleted temporary CSV file: {file_path_to_delete}")
            except OSError as e:
                logging.error(f"Error deleting file {file_path_to_delete}: {e}")

        messagebox.showinfo("Process Complete", "Data export, zipping, and cleanup finished.")
        logging.info("Full export process completed successfully.")

    except mysql.connector.Error as err:
        logging.error(f"MySQL connection error: {err}")
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            messagebox.showerror("Database Error",
                                 "Access denied for database. Check user/password in export_config.json.")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            messagebox.showerror("Database Error", "Database does not exist. Check DB_NAME in export_config.json.")
        else:
            messagebox.showerror("Database Error", f"An unexpected database error occurred:\n{err}")
    except Exception as e:
        logging.critical(f"An unhandled error occurred during export_to_csv: {e}", exc_info=True)
        messagebox.showerror("Critical Error",
                             f"An unexpected error occurred during export:\n{e}\nCheck export_tool.log for details.")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            logging.info("Database connection closed in finally block.")
        progress['value'] = 0
        progress.grid_remove()


def run_query():
    selected_month = combo_month.get()
    selected_year = entry_year.get()

    if not selected_month or not selected_year:
        messagebox.showwarning("Input Error", "Please select both a month and a year.")
        return

    month = month_mapping.get(selected_month)
    try:
        year = int(selected_year)
    except ValueError:
        messagebox.showerror("Input Error", "Invalid year. Please enter a valid 4-digit year.")
        return

    ethiopian_date = EthDate(20, month, year)

    gregorian_end_date = to_gregorian(ethiopian_date)

    # Calculate Gregorian start date (21st of previous month in Ethiopian calendar)
    if month == 1:  # If Meskerem (month 1), previous Ethiopian month is Puagume (month 13 of previous year)
        gregorian_start_date = to_gregorian(EthDate(21,13,year-1))  # Correct for Puagume
    else:
        gregorian_start_date = to_gregorian(EthDate(21,month-1,year))

    queries_to_execute = {}
    for tag, path in QUERY_FILES.items():
        query_content = read_sql_file_content(path)
        if query_content:
            queries_to_execute[tag] = query_content

    if queries_to_execute:
        export_to_csv(queries_to_execute, gregorian_start_date, gregorian_end_date)
    else:
        messagebox.showerror("Error", "No valid queries found in export_config.json or SQL files are missing.")
        logging.error("No valid queries found or SQL files are missing.")


# --- UI Components Month ---
tk.Label(root, text="Select Month:").grid(row=0, column=0, pady=5, padx=10, sticky="e")
combo_month = ttk.Combobox(root, values=months, state="readonly", width=25)
combo_month.grid(row=0, column=1, padx=10, pady=5, sticky="w")
combo_month.set(months[0])

# --- UI Components Year ---
tk.Label(root, text="Year (YYYY):").grid(row=1, column=0, pady=5, padx=10, sticky="e")
entry_year = ttk.Combobox(root, values=years, state="readonly", width=25)
entry_year.grid(row=1, column=1, padx=10, pady=5, sticky="w")
print("Date time today",datetime.today())
print("Ethiopian Date ",to_ethiopian(datetime.today()))
# Set a default year
if str(to_ethiopian(datetime.today()).year) in years:
    entry_year.set(str( to_ethiopian(datetime.today()).year))
else:
    entry_year.set(years[-1])

run_button = ttk.Button(root, text="Run Export", command=run_query)
run_button.grid(row=2, column=0, columnspan=2, pady=10)

for i in range(3):
    root.grid_rowconfigure(i, weight=1)
root.grid_columnconfigure(0, weight=1)
root.grid_columnconfigure(1, weight=1)
root.grid_rowconfigure(6, weight=1)


if __name__ == "__main__":
    logging.info("Application starting.")
    root.mainloop()
    logging.info("Application closed.")