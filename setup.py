from cx_Freeze import setup, Executable
import sys

# Include additional files (e.g., config and SQL queries)
include_files = [
    ("export_config.json", "export_config.json"),
    ("sql_queries", "sql_queries")
]

# Dependencies required for your project
packages = [
    "tkinter",
    "csv",
    "os",
    "sys",
    "json",
    "hashlib",
    "zipfile",
    "glob",
    "logging",
    "mysql.connector",
    "datetime",
    "ethiopian_date_converter"
]

# Platform-specific options
build_options = {
    "packages": packages,
    "include_files": include_files,
    "build_exe": "export_distributable",
    "excludes": ["unittest", "pydoc", "test"]
}

# Add Linux-specific includes for tkinter
if sys.platform == "linux":
    build_options["include_files"] = include_files

base = None
if sys.platform == "win32":
    base = "Win32GUI"

setup(
    name="ExportTool",
    version="1.0",
    description="Data extraction Tool",
    options={
        "build_exe": build_options
    },
    executables=[Executable("export.py", base=base)]
)
