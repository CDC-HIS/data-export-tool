from cx_Freeze import setup, Executable


# Include additional files (e.g., config and SQL queries)
include_files = [
    ("export_config.json", "export_config.json"),  # Config file
    ("sql_queries", "sql_queries")                 # Folder with SQL files
]

# Dependencies required for your project
packages = ["pytds", "os", "json", "csv", "glob", "shutil", "hashlib", "zipfile", "logging"]

setup(
    name="ExportTool",
    version="1.0",
    description="Data extraction Tool",
    options={
        "build_exe": {
            "packages": packages,
            "include_files": include_files,
            "build_exe": "export_distributable"      # Output folder
        }
    },
    executables=[Executable("export.py", base=None)]
)
