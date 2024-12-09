# Steps to create distributable
### Install Pyinstaller
`pip install pyinstaller`

### (Optional) Generate release spec file if it doesn't exist
`pyinstaller --onefile --name import_tool import.py`
### (Optional)  edit import_tool.spec file to include import_config.json file
`datas=[('import_config.json', '.')],`
### Generate program with 
`pyinstaller import_tool.spec` 