#!/usr/bin/env python3

import os; os.environ["CONDOR_CONFIG"] = "/dev/null"  # squelch warning
import sys
from datetime import timedelta
import accounting
from accounting.push_totals_to_es import push_totals_to_es
import json
import traceback

#settings = {
#    "es_index": "osg-schedd-*",
#    "to": ["jpatton@cs.wisc.edu"],
#}

args = accounting.parse_args(sys.argv[1:])

filtr = args.filter(**vars(args))
raw_data = filtr.get_filtered_data()

with open("last_data.json", "w") as f:
    json.dump(raw_data, f, indent=2)

table_names = list(raw_data.keys())
csv_files = {}
for table_name in table_names:
    table_data = filtr.merge_filtered_data(filtr.get_filtered_data(), table_name)
    csv_files[table_name] = accounting.write_csv(table_data, filtr.name, table_name, **vars(args))

table_files = [csv_files[name] for name in ["Projects", "Users", "Schedds", "Site"] if name in csv_files]
formatter = args.formatter(table_files, **vars(args))
html = formatter.get_html()
with open("last_html.html", "w") as f:
    f.write(html)

try:
    accounting.send_email(
        subject=formatter.get_subject(**vars(args)),
        html=html,
        table_files=table_files,
        **vars(args))
except Exception:
    traceback.print_exc()

push_totals_to_es(table_files, "daily_totals", **vars(args))
