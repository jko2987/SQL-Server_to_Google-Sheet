import sys
import os
import pkg_resources
import setuptools
from cx_Freeze import setup, Executable

site_pkg = os.listdir('site-packages')
site_pkg_dir = os.getcwd() + '/site-packages/'


# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {"packages": ['os', 'sys', 're', 'pyodbc', 'pandas', 'google.oauth2', 'pkg_resources', 'httplib2', 'numpy','six', 'pytz', 'dateutil', 'requests', 'gspread', 'socket'],\
                     'include_files': ['db_map.json', 'gsheet_creds.json', 'gcp-sa.json', 'README.txt', 'log/'] + [site_pkg_dir + x for x in site_pkg]}


# GUI applications require a different base on Windows (the default is for a
# console application).
base = None

setup(
    name = "sql_to_gsheet",
    version = "0.3",
    description = "SQL to GSheet Automation",
    options = {"build_exe": build_exe_options},
    executables = [Executable("sql_to_gsheet.py", base=base)],
)