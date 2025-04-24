CHTC Elasticsearch DSL Reporting

### Install Required Packages
* `pip install elasticsearch<8 elasticsearch_dsl<8 tabulate dnspython`

### Command Line Options
#### Output Options
* `--output` specify the output CSV file name
* `--print-table` prints the report output on the command line

#### Elasticsearch Options
* `-s / --start <YYYY-MM-DD>` Report from this date forward
* `-e / --end <YYYY-MM-DD>` Report until this date (defaults to now)
* `--es-host <hostname>` Elasticsearch server hostname (defaults to `localhost:9200`)
* `--es-agg-by <field>` The Elasticsearch field name to aggregate under (ex. ProjectName, User, ScheddName, ...)
* `--es-url-prefix <prefix>` The URL prefix for the Elasticsearch server (defaults to 'http://' or 'https://' with --es-use-https)
* `--es-index <index_name>` Elasticsearch index to search from (defaults to `chtc-schedd-*`)
* `--es-user <username>` Elasticsearch username (must also specify --es-password-file)
* `--es-password-file <path>` A file containing an Elasticsearch password (must also specify --es-user)
* `--es-use-https` Toggle HTTPS (requires either --es-ca-certs or that `certifi` is installed)
* `--es-ca-certs` Provide ca-certs
* `--es-config-file` provide the above options in a JSON config file instead
