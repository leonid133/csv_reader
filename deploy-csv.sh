#!/bin/bash

CONFIGS_PATH='deploy'
USER='dev'

virtualenv .
source ./bin/activate
pip install mist-cli


mist-cli --host localhost --port 2004 apply --file $CONFIGS_PATH --validate false

#show status after deploy
mist-cli --host localhost --port 2004 -f list functions
mist-cli --host localhost --port 2004 -f list contexts

deactivate