# Steps to create distributable
### Avail TK Inter in the environment
`sudo apt install python3-tk`
### Install CX Freeze
`pip install cx_Freeze`

### Build Release
`python setup.py build`

export_distributable directory will be created

# Nutika Packaging
`python3 -m nuitka \
    --onefile \
    --standalone \
    --enable-plugin=tk-inter \
    --include-data-file=export_config.json=. \
    --include-data-dir=sql_queries=sql_queries \
    --output-dir=build \
    --output-filename=export_tool \
    export_debug.py
`