# Elasticsearch Job Accounting Scripts

## Install

`git clone` the repository.

## Usage

You can either work out of the cloned source directory,
or you add the cloned source directory to `PYTHONPATH`
and copy the `send_email.py` script to another working directory.

The main script is `send_email.py`:

```$ python send_email.py --help
usage: send_email.py [-h] [--debug] [--quiet] [--restart] [--log_file LOG_FILE] [--do_not_upload] [--filter FILTER] [--formatter FORMATTER] [--es_index ES_INDEX]
                     [--es_host ES_HOST] [--es_user ES_USER] [--es_use_https] [--es_ca_certs ES_CA_CERTS] [--daily] [--weekly] [--monthly] [--start_ts START_TS]
                     [--end_ts END_TS] [--csv_dir CSV_DIR] [--from_addr FROM_ADDR] [--to_addr TO_ADDR] [--cc_addr CC_ADDR] [--bcc_addr BCC_ADDR]
                     [--reply_to_addr REPLY_TO_ADDR]

options:
  -h, --help            show this help message and exit
  --debug               Log debug messages
  --quiet               Do not print log messages
  --restart             Try restarting from pickled data on disk
  --log_file LOG_FILE   Set an optional log file
  --do_not_upload       Do not store output in ES
  --filter FILTER       Filter class, one of [BaseFilter, ChtcScheddCpuFilter, ChtcScheddCpuMonthlyFilter, ChtcScheddCpuRemovedFilter, ChtcScheddGpuFilter,
                        ChtcScheddJobDistroFilter, OsgScheddCpuFilter, OsgScheddCpuHeldFilter, OsgScheddCpuMonthlyFilter, OsgScheddCpuRemovedFilter, OsgScheddCpuRetryFilter,
                        OsgScheddJobDistroFilter, OsgScheddLongJobFilter, PathScheddCpuFilter] (default: BaseFilter)
  --formatter FORMATTER
                        Formatter class, one of [BaseFormatter, ChtcScheddCpuFormatter, ChtcScheddCpuRemovedFormatter, ChtcScheddGpuFormatter, ChtcScheddJobDistroFormatter,
                        OsgScheddCpuFormatter, OsgScheddCpuHeldFormatter, OsgScheddCpuRemovedFormatter, OsgScheddCpuRetryFormatter, OsgScheddJobDistroFormatter,
                        OsgScheddLongJobFormatter, PathScheddCpuFormatter] (default: BaseFormatter)
  --es_index ES_INDEX   Elasticsearch index (default: htcondor-000001)
  --es_host ES_HOST     Elasticsearch hostname[:port] (default: localhost)
  --es_user ES_USER     Elasticsearch username
  --es_use_https        Use HTTPS with Elasticsearch (default: ES_USE_HTTPS=False)
  --es_ca_certs ES_CA_CERTS
                        Elasticsearch custom CA certs
  --daily               Set report period to last full day (REPORT_PERIOD=daily, default)
  --weekly              Set report period to last full week (REPORT_PERIOD=weekly)
  --monthly             Set report peroid to last full month (REPORT_PERIOD=monthly)
  --start_ts START_TS   Custom starting timestamp
  --end_ts END_TS       Custom ending timestamp
  --csv_dir CSV_DIR     Output directory for CSVs (default: csv)
  --from_addr FROM_ADDR
                        From: email address (default: accounting@accounting.chtc.wisc.edu)
  --to_addr TO_ADDR     To: email address (can be specified multiple times)
  --cc_addr CC_ADDR     CC: email address (can be specified multiple times)
  --bcc_addr BCC_ADDR   BCC: email address (can be specified multiple times)
  --reply_to_addr REPLY_TO_ADDR
                        Reply-To: email address
```

This is best wrapped in a shell script that sets up the required
Python environment. Using an existing Miniconda environment named
`job_accounting`, for example, you could have a script like:

```
#!/bin/bash

# Set up the Python environment
source ~/miniconda3/bin/activate
conda activate job_accounting
pip install -r requirements.txt

# Send an OSPool email to address1@site.com and address2@site.com
python3 send_email.py \
    --daily \
    --es_index="osg-schedd-*" --filter=OsgScheddCpuFilter --formatter=OsgScheddCpuFormatter \
    --to_addr="address1@site.com" \
    --to_addr="address2@site.com"
```

## Modification

There are base filtering and formatting classes along with child
classes related to the OSPool report in
`accounting/filters` and `accounting/formatters`.
Filter classses contain methods that query Elasticsearch for specific
fields, and contain methods that use the resulting data to derive output fields
(like `All CPU Hours`).
Formatting classes contain methods that take a CSV file
and turns the data in the CSV file into nicely formatted HTML.

Changes to the OSPool report should only require changes to the
OSPool-specific classes (`OsgScheddCpuFilter` and
`OsgScheddCpuFormatter`).

If you need to create a new type of report, create new filter and
format classes, using the existing classes as reference. Be sure to
add any new classes to the `__init__.py` in the `filters` and
`formatters` directories, and update `config.py` with the new class
names if you want them to show up in the `send_email.py --help`
message.
