#!/bin/bash
# Installation script for pheno_db database loading application.
# Developed to be able to simplify deployment of application in jupyter hub environment

parentdir="$(dirname "$(pwd)")"

set -e

sudo apt-get install software-properties-common


sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update -y
sudo apt-get install python3.7 -y

sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1
sudo update-alternatives --config python3

sudo apt-get install python-pip -y
sudo apt-get install libfreetype6-dev -y

##to fix psycopg2 install error
sudo apt-get install libpq-dev -y
sudo apt-get install python3.6-dev -y
sudo apt-get install python3.7-dev -y


#update pip to prevent errors in virtualenv install
pip install -U pip

# otherwise virtualenv install will fail on zipp module
pip install setuptools

pip install virtualenv

echo "setting up virtual environment and installing dependencies..."

cd $parentdir/pheno_db
virtualenv -p python3 .venv 
source $parentdir/pheno_db/.venv/bin/activate

pip install -r requirements.txt
deactivate

echo "Installation complete.  Please create a .env file with database connection and loadfile information before running the script."