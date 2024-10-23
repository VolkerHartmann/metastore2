#!/bin/bash
################################################################################
# Run microservice 'metastore2'
# /
# |- application.properties    - Default configuration for microservice
# |- 'metastore2'*.jar         - Microservice
# |- run.sh                    - Start script
# |- lib/                      - Directory for plugins (if supported)
# |- config/
#    |- application.properties - Overwrites default configuration (optional)
################################################################################
 
################################################################################
# Define jar file
################################################################################
jarFile=metastore2.jar
 
################################################################################
# Determine directory of script.
################################################################################
ACTUAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$ACTUAL_DIR"
 
################################################################################
# Start micro service
################################################################################
java -cp ".:$jarFile" -Dloader.path="file://$ACTUAL_DIR/$jarFile,./lib/,." -jar $jarFile  --migrate2DataCite
