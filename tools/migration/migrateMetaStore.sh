#!/bin/bash
################################################################################
# Global variables
################################################################################
TMP_DIR=/tmp
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# Installation dir of MetaStore should be 2 levels higher (../..)
INSTALLATION_DIR="$( cd "$( dirname $( dirname "${SCRIPT_DIR}" ))" >/dev/null 2>&1 && pwd )"

################################################################################
function printInfo {
################################################################################
echo "-------------------------------------------------------------------------"
echo $*
echo "-------------------------------------------------------------------------"
}
################################################################################
function getLocation {
################################################################################
  printInfo "Get Location..."

#IFS=',' read -ra ADDR <<< "$RESPONSE"
readarray -t lines <<< "$RESPONSE"
for i in "${lines[@]}"; do
echo $i |grep Location >> /dev/null
## get status ##
if [ $? -eq 0 ]
then
  IFS=' ?' read -ra ADDR <<< "$i"
  LOCATION=`echo ${ADDR[1]} | tr -d '\r'`
fi
done
}
################################################################################
function getValueFromProperties {
# Parameter:
#  - Separator(s)
#  - Index
#  - Property
# e.g.: getValueFromProperties := -1 server.port
################################################################################
# First try to find key in config/application.properties.
# If this fails search in application.properties.
IFS="$1" read -r -a array <<< "$(grep -iR "^$3" $INSTALLATION_DIR/config/application.properties)"
if [ -n "$array" ]; then
    PROPERTY_VALUE=${array[$2]}
else
    IFS="$1" read -r -a array <<< "$(grep -iR "^$3" $INSTALLATION_DIR/application.properties)"
    PROPERTY_VALUE=${array[$2]}
fi
}
################################################################################
# End of functions
################################################################################
if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
    echo "USAGE:"
    echo "  ${BASH_SOURCE[0]} oldURL newURL"
    echo "e.g.:"
    echo "${BASH_SOURCE[0]} https://domain:port http://domain/context-path"
    exit
fi
# Determine directories of script and of the MetaStore installation.
echo "Script dir: " $SCRIPT_DIR
echo "Installation dir: " $INSTALLATION_DIR

getValueFromProperties "=: " "-1" "spring.datasource.url"
DATABASE_URL="${PROPERTY_VALUE#"${PROPERTY_VALUE%%[![:space:]]*}"}"
#printInfo "Database URL: $DATABASE_URL"

getValueFromProperties "=: " "2" "spring.datasource.url"
DATABASE="${PROPERTY_VALUE#"${PROPERTY_VALUE%%[![:space:]]*}"}"
#printInfo "Database: $DATABASE"

if [ "$DATABASE" == "h2" ]; then
  DATABASE_URL=jdbc:h2:file:$DATABASE_URL
else
  getValueFromProperties "/" "-1" "spring.datasource.url"
  DATABASE="${PROPERTY_VALUE#"${PROPERTY_VALUE%%[![:space:]]*}"}"
fi
# Get user and password (only for h2 needed)
getValueFromProperties "=: " "-1" "spring.datasource.username"
DATABASE_USER="${PROPERTY_VALUE#"${PROPERTY_VALUE%%[![:space:]]*}"}"

getValueFromProperties "=: " "-1" "spring.datasource.password"
DATABASE_PASSWORD="${PROPERTY_VALUE#"${PROPERTY_VALUE%%[![:space:]]*}"}"

ORIG_URL=$1
NEW_URL=$2

cd "$SCRIPT_DIR"
printInfo "Migrate Metastore from '$ORIG_URL' to '$NEW_URL'"
if [ "$DATABASE" == "h2" ]; then
  printInfo "URL: '$DATABASE_URL', USER: '$DATABASE_USER', PASSWORD: '$DATABASE_PASSWORD'"
  DATABASE=PUBLIC
  cp migration_template.sql migration.sql
else
  printInfo "HOSTNAME: localhost, DATABASE: '$DATABASE'"
  cp migration_postgresql_template.sql migration.sql
fi

sed -i 's?${DATABASE}?'$DATABASE'?g' migration.sql
sed -i 's?${OLD_URL}?'$ORIG_URL'?g' migration.sql
sed -i 's?${NEW_URL}?'$NEW_URL'?g' migration.sql

if [ $DATABASE == "h2" ]; then
  java -cp "$ACTUAL_DIR"/lib/h2-2.1.214.jar org.h2.tools.RunScript -script migration.sql -user $DATABASE_USER -password $DATABASE_PASSWORD -url "$DATABASE_URL"
else
  # First backup database
  sudo -u postgres pg_dump metastore > dump_metastore_`date +%Y_%m_%dt%H_%M`.sql
  # Make file readable for all
  cp migration.sql $TMP_DIR/migration.sql
  sudo -u postgres psql -f $TMP_DIR/migration.sql
fi
exit
