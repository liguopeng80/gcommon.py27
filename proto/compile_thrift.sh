#!/bin/bash

if [ -f ./error_define.thrift ]; then
    echo "Compiling error_define.thrift"
    thrift -out ./gen-py --gen py:twisted -v slim_error_define.thrift
    echo "Done"
    echo ""
else
    echo "No error definition file found, generating"
    echo ${PYTHONPATH}
    export PYTHONPATH=../:${PYTHONPATH}
    echo ${PYTHONPATH}
    python ../app/slim_error_generator.py -f slim_error_define.thrift
    echo "Done"
    echo ""

    echo "Compiling error_define.thrift"
    thrift -out ./gen-py --gen py:twisted -v slim_error_define.thrift
    echo "Done"
    echo ""
fi
echo "Compiling postman.thrift"
thrift -out ./gen-py --gen py:twisted -v postman.thrift
echo "Done"
echo ""
