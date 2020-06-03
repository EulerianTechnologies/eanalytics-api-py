from setuptools import setup
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    author = 'Florian',
    author_email = 'f.dauphin@eulerian.com',
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent"
    ],
    description = 'Locally download a datamining dataset from the Eulerian Technologies API',
    download_url = 'https://github.com/EulerianTechnologies/eanalytics-api-py/archive/master.zip',
    install_requires=['requests>=2.23.0', 'ijson>=3.0.4',],
    keywords = ['eulerian', 'datamining', 'download', 'dw', 'datawharehouse', 'eanalytics'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    name = 'eanalytics_api_py',
    packages = ['eanalytics_api_py'],
    platforms = ['any'],
    python_requires='>=3',
    url = 'https://github.com/EulerianTechnologies/eanalytics-api-py',
    version = '0.0.61',
)
