# Shell script to migrate MetaStore to another location
There may be some reasons to migrate MetaStore to another location. 
These are (with examples):
- Switching from http to https
  - bash migrateMetaStore.sh http://domain:port https://domain:port
- Switching from domain1 to domain2
  - bash migrateMetaStore.sh https://domain1 https://domain2
- Move MetaStore behind a proxy
  - bash migrateMetaStore.sh http://domain:port https://proxy/context-path
- Switch context path
  - bash migrateMetaStore.sh https://domain/context-path-old https://proxy/context-path-new
- Move to a new domain

The provided shell script generates a SQL script and perform this to the
*local* database. 
It's tested with file based h2 database and a local postgres database.
However, there are some pitfalls that need to be considered:
### File Based H2 Database
Access rights to the file are sufficient.
### Postgres
For postgres you may execute the shell script as *root*. Otherwise
(e.g. ubuntu) you are asked for the sudo password during execution.
For databases which are not locally installed please manually execute 'migration.sql'
on database.

This may look like this:
```
$ psql -h host -U user -d database -f migration.sql

host: hostname of the database server
user: username of the database (default: metastore_admin)
database: database for MetaStore (default: metastore)
```

### Other Databases
Generated SQL might not work with your database. Therefor you may have to modify 
'migration.sql' to your needs.

