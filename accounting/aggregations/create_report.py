# driver code to run the reporting script
# parses cmd line args and handles outputs

import elasticsearch
from elasticsearch_dsl import Search, Q, A, connections
import argparse
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from pprint import pprint

from query import run_query
from functions import send_email 
from report_helpers import Aggregation, add_runtime_script, get_percent_bucket_script, table, print_error

OUTPUT_ARGS = {
    "--print-table" : {"action" : "store_true", "help" : "prints a CLI table, NOTE: pipe into 'less -S'"},
    "--output"      : {"default" : f"{datetime.now().strftime("%Y-%m-%d:%H:%M")}-report.csv",
                       "help" : "specify the CSV output file name, defaults to '<date:time>-report.csv'"},
    "--emit-html"   : {"action" : "store_true", "help" : "writes generated HTML to a file"}
}

EMAIL_ARGS = {
    "--no-email" : {"action" : "store_true", "help" : "don't send an email"},
    "--from": {"dest": "from_addr", "default": "no-reply@chtc.wisc.edu"},
    "--reply-to": {"default": "ospool-reports@g-groups.wisc.edu"},
    "--to": {"action": "append", "default": []},
    "--cc": {"action": "append", "default": []},
    "--bcc": {"action": "append", "default": []},
    "--smtp-server": {"default" : "smtp.wiscmail.wisc.edu"},
    "--smtp-username": {},
    "--smtp-password-file": {"type": Path}
}

ELASTICSEARCH_ARGS = {
    "--es-host": {"default" : "localhost:9200"},
    "--es-agg-by" : {"default" : "ProjectName.keyword", "nargs" : "+", "help" : "generate 1 or more tables aggregated by an ES field"},
    "--es-url-prefix": {"default" : "http://"},
    "--es-index": {"default" : "chtc-schedd-*"},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    }
}

# =========== helper functions ===========

def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--start", type=valid_date, required=True, 
                        help="the date to start reporting on 'YYYY-MM-DD' (REQUIRED)")
    parser.add_argument("--end", type=valid_date, default=datetime.strftime(datetime.now(), "%Y-%m-%d"),
                        help="the date to end reporting on 'YYYY-MM-DD', defaults to the current date")

    email_opts = parser.add_argument_group("email options")
    for name, props in EMAIL_ARGS.items():
        email_opts.add_argument(name, **props)

    es_opts = parser.add_argument_group("elasticsearch options")
    for name, props in ELASTICSEARCH_ARGS.items():
        es_opts.add_argument(name, **props)

    output_opts = parser.add_argument_group("output options")
    for name, props in OUTPUT_ARGS.items():
        output_opts.add_argument(name, **props)

    return parser.parse_args()

def get_client(args: dict) -> elasticsearch.Elasticsearch:
    # set up elasticsearch options
    es_opts = {
        "timeout" : 120,
        "hosts" : [args["es_url_prefix"] + args["es_host"]], 
    }

    if args["es_use_https"]:
        if args["es_ca_certs"]:
            es_opts.update({"ca_certs" : str(args["es_ca_certs"])})
        
        elif importlib.util.find_spec("certifi") is None:
            print("error: using HTTPS requires either ca_certs or the certifi library to be installed")
            sys.exit(1)

        es_opts.update({"hosts" : "https://" + args["es_host"]})
        es_opts.update({"use_https" : True})
        es_opts.update({"verify_certs" : True}) 

    if (not args["es_user"]) ^ (not args["es_password_file"]):
        print("error: you must specify a username and password, or neither")
        sys.exit(1)

    elif args["es_user"] and args["es_password_file"]:
        passwd_str = ""    
        with open(args["es_password_file"], 'r') as file:
           passwd_str = file.read()
 
        es_opts.update({"http_auth" : (args["es_user"], passwd_str)})

    client = elasticsearch.Elasticsearch(**es_opts)
    connections.create_connection(alias="default", client=client)
   
    return client

def main():
    # parse arguments
    args = parse_args()

    # read the config file if there is one
    es_opts = {}
    if args.es_config_file:
        es_opts = json.load(args.es_config_file.open())
    else:
        es_opts = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
  
    client = get_client(es_opts)

    # cast agg_by to a list if not already
    if not isinstance(args.es_agg_by, list):
        args.es_agg_by = [args.es_agg_by]
     
    results = []
    for field in args.es_agg_by:
        results.append((field, run_query(client, es_opts, args, field)))

    # generate the outputs
    if args.print_table: 
        for title, r in results:
            print('\n')            
            print(f"CHTC Jobs by {title.split('.')[0]} for {args.start.strftime('%Y-%m-%d %H:%M:%S')} TO {args.end.strftime('%Y-%m-%d %H:%M:%S')}")
            print('got here')
            table(r)
            
    # send an email
    html_tables = []
    for title, r in results:
        header_str = f"<h3>CHTC Jobs by {title.split('.')[0]} from {args.start.strftime('%Y-%m-%d %H:%M:%S')} TO {args.end.strftime('%Y-%m-%d %H:%M:%S')}</h3>"
        html_tables.append((header_str, table(r, emit_html=True)))

    # add css to make the table look pretty
    # NOTE: the alternating row colors don't render in outlook
    # but they will if the html is opened in a browser
    html = f"""
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th {{
                white-space: nowrap;
                background-color: #d6d6d6;
            }}
            th, td {{
                padding: 10px;
                text-align: left;
                border: 1px solid black;
            }}
            table tr:nth-child(even) td{{
                background-color: #fce3f4;
            }}
        </style>
    """

    # add the tables
    for hdr, t in html_tables:
        html += (hdr + '\n' + t)

    # write an HTML file if the arg is set
    if args.emit_html:
        with open("table.html", 'w') as htmlfile:
            htmlfile.write(html)

    # abort email if tabulate is not installed
    if html is None and not args.no_email:
        sys.exit(1)

    if not args.no_email:    
        send_email(
            subject=f"{(args.end - args.start).days}-day CHTC Usage Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
            from_addr=args.from_addr,
            to_addrs=args.to,
            html=html,
            cc_addrs=args.cc,
            bcc_addrs=args.cc,
            reply_to_addr=args.reply_to,
            smtp_server=args.smtp_server,
            smtp_username=args.smtp_username,
            smtp_password_file=args.smtp_password_file,
        )

if __name__ == "__main__":
    main()
