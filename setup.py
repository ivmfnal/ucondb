import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), "r").read()

def get_version():
    g = {}
    exec(open(os.path.join("ucondb", "version.py"), "r").read(), g)
    return g["Version"]


setup(
    name = "ucondb",
    version = get_version(),
    author = "Igor Mandrichenko",
    author_email = "ivm@fnal.gov",
    description = ("Unstructured Conditions Database (UConDB)"),
    license = "BSD 3-clause",
    keywords = "database, web service, conditions database, blob storage",
    packages=['ucondb', 'ucondb.tools', 'ucondb.backends', 'ucondb.ui'],
    zip_safe = False,
    classifiers=[
    ],
    entry_points = {
            "console_scripts": [
                "ucondb = ucondb.ui.ui:main",
            ]
        }
)