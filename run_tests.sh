#!/bin/bash

# The following method is cross-platform (OS X and Linux)
MYDIR=$(dirname $(python -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' $0))
PY2="/usr/bin/env python2"
PY3="/usr/bin/env python3"
PY2_VENV=${MYDIR}/.venv2
PY3_VENV=${MYDIR}/.venv3

if [ ! -d $PY2_VENV ]; then
    $PY2 -mvirtualenv $PY2_VENV
fi

if [ ! -d $PY3_VENV ]; then
    $PY3 -mvenv $PY3_VENV
fi

. ${PY2_VENV}/bin/activate
pip install -r ${MYDIR}/test-requirements.txt
PYTHONPATH=$MYDIR pytest ${MYDIR}/tests/test_collectd_plugin.py
deactivate

. ${PY3_VENV}/bin/activate
pip install -r ${MYDIR}/test-requirements.txt
PYTHONPATH=$MYDIR pytest ${MYDIR}/tests/test_collectd_plugin.py
deactivate
