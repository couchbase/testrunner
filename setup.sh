# This scripts attempts to setup an environment for running testrunner tests.
# 1. Check that you have either Python 2.7 or python 3 installed along with virtualenv
# 2. Installs venv in this directory
# 3. Installs all pip packages required by this repo
# 4. Adds custom library paths to your PYTHONPATH

helpFunction()
{
   echo ""
   echo "Usage: $0 <couchbase_lib> "
   echo -e "setup.sh couchbase3 or setup.sh couchbase2"
   exit 1 # Exit script after printing help
}

if [[ $# -lt 1 ]]; then
    echo "Unexpected arguments passed. Check --help."
fi

PYTHON=python3
PIP=pip3
if [[ $1 == couchbase3 ]]; then
    echo "Using Couchbase 3 SDK "
    sed -i 's/couchbase~=2.5.12/couchbase~=3.0.0/g' requirements.txt

elif [[ $1 == couchbase2 ]]; then
    echo "Using Couchbase 2 SDK"

else
    echo "Exiting. Make sure Couchbase version is either 3 or 2"
    exit 1
fi

$PYTHON -m virtualenv --version
if [ $? -ne 0 ]; then
    # Install virtual env
    echo "Virtualenv not detected, running '$PIP install virtualenv'.  If you don't have $PIP, run easy_install $PIP"
    $PIP install virtualenv
    if [ $? -ne 0 ]; then
        echo "$PIP not detected. Install $PIP. Running 'easy_install $PIP'"
        easy_install $PIP
    fi

fi

currentdir=$(pwd)
export PATH=$PATH:/usr/local/bin

# Setup virtual env
virtualenv -p $PYTHON venv
source venv/bin/activate

# Install PYTHON dependencies
$PIP install -r requirements.txt

# set PYTHON env
export PYTHONPATH=$PYTHONPATH:$currentdir/