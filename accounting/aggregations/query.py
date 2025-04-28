import elasticsearch
import argparse
import sys
import json
from functions import send_email
from elasticsearch_dsl import Search, Q, A, connections
from datetime import datetime, timedelta
from collections import namedtuple
from operator import itemgetter
from pprint import pprint
from pathlib import Path

from report_helpers import Aggregation, add_runtime_script, get_percent_bucket_script, table, print_error

# lists to hold aggregation objects
ROWS_AGGS = []
TOTALS_AGGS = []

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
    "--es-agg-by" : {"default" : "ProjectName.keyword"},
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


# percentages for the totals row is calculated in python
# due to limitations with calculating percents in ES
def calc_totals_percents(resp) -> dict:
    ret = {}
    buckets = resp.aggregations.to_dict()

    # TODO what to do about the previously stored pretty names?
    ret.update({"% Goodput" : 
                ((buckets["good_core_hours"]["value"] / buckets["cpu_core_hours"]["value"]) if buckets["cpu_core_hours"]["value"] else 0) * 100}) 
    ret.update({"% Ckptable" : 
                ((buckets["ckptable_filt"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0) * 100})
    ret.update({"% Removed" : 
                ((buckets["rmd_filt"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["cpu_core_hours"]["value"] else 0) * 100})
    ret.update({"Shadow Starts / ID" : 
                ((buckets["num_shadw_starts"]["value"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0)})
    ret.update({"Exec Att / Shadow Start" : 
                ((buckets["num_exec_attempts"]["value"] / buckets["num_shadw_starts"]["value"]) if buckets["num_shadw_starts"]["value"] else 0)})
    ret.update({"Holds / ID" : 
                ((buckets["num_holds"]["value"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0)})
    ret.update({"% Short" : 
                ((buckets["short_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0) * 100})
    ret.update({"% Restarted" : 
                ((buckets["restarted_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0) * 100})
    ret.update({"% Held" : 
                ((buckets["held_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0) * 100})
    ret.update({"% Over Req. Disk" : 
                ((buckets["over_disk_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0) * 100})
    ret.update({"% S'ty Jobs" : 
                ((buckets["sty_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"]) if buckets["uniq_job_ids"]["value"] else 0) * 100})

    return ret

def get_client(args: dict):
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

# =========== end of helper functions ===========

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
 
    # nicely the Q object supports ~ for negation :)
    # 'search' is aggregated by project
    search = Search(using=client, index=es_opts["es_index"]) \
                    .filter("range", RecordTime={"gte" : args.start.timestamp(), "lt" : args.end.timestamp()}) \
                    .filter(~Q("terms", JobUniverse=[7, 12])) \
                    .filter("wildcard", **{"ScheddName.keyword": {"value": "*.chtc.wisc.edu"}}) \
                    .extra(size=0) \
                    .extra(track_scores=False)

    # totals query is exactly the same
    totals = Search(using=client, index=es_opts["es_index"]) \
                    .filter("range", RecordTime={"gte" : args.start.timestamp(), "lt" : args.end.timestamp()}) \
                    .filter(~Q("terms", JobUniverse=[7, 12])) \
                    .filter("wildcard", **{"ScheddName.keyword": {"value": "*.chtc.wisc.edu"}}) \
                    .extra(size=0) \
                    .extra(track_scores=False)

    # top level aggregation is by project
    search.aggs.bucket(
        "projects", "terms",
        field=es_opts["es_agg_by"],
        size=1024
    )

    # =========== aggregations section ===========

    # count unique users
    # search.aggs["projects"].metric("uniq_users", "cardinality", field="User.keyword")
    ROWS_AGGS.append(Aggregation(
                    A("cardinality", field="User.keyword"),
                    "uniq_users",
                    "# Users",
                    "metric",
    ))

    # can be resused for totals
    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # count total jobs for internal use
    search.aggs["projects"].metric("total_jobs", "value_count", field="GlobalJobId.keyword");
    totals.aggs.metric("total_jobs", "value_count", field="GlobalJobId.keyword");

    # count unique job ids
    #search.aggs["projects"].metric("uniq_job_ids", "cardinality", field="GlobalJobId.keyword")
    ROWS_AGGS.append(Aggregation(
                    A("cardinality", field="GlobalJobId.keyword"),
                    "uniq_job_ids",
                    "# Jobs",
                    "metric",
    ))

    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # count total CPU hours, and % goodput hours
    CPU_CORE_HOURS_SCRIPT_SRC = """
        double hours = 0;
        int cpus = 1;
        if (doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {
            hours = (double)doc["RemoteWallClockTime"].value / (double)3600;
        }
        if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
            cpus = (int)doc["RequestCpus"].value;
        }
        emit((double)cpus * hours);
    """

    GOOD_CPU_HOURS_SCRIPT_SRC = """
        double hours = 0;
        int cpus = 1;
        if (doc.containsKey("lastremotewallclocktime.keyword") && doc["lastremotewallclocktime.keyword"].size() > 0) {
            hours = Double.parseDouble(doc["lastremotewallclocktime.keyword"].value) / (double)3600;
        }
        if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
            cpus = (int)doc["RequestCpus"].value;
        }
        emit((double)cpus * hours);
    """

    add_runtime_script(search, "CpuCoreHours", CPU_CORE_HOURS_SCRIPT_SRC, "double")
    add_runtime_script(search, "GoodCpuCoreHours", GOOD_CPU_HOURS_SCRIPT_SRC, "double")

    add_runtime_script(totals, "CpuCoreHours", CPU_CORE_HOURS_SCRIPT_SRC, "double")
    add_runtime_script(totals, "GoodCpuCoreHours", GOOD_CPU_HOURS_SCRIPT_SRC, "double")

    ROWS_AGGS.append(Aggregation(
                    A("sum", field="CpuCoreHours"),
                    "cpu_core_hours",
                    "CPU Hours",
                    "metric",
    ))

    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # these aggs are added dicrectly to the query
    # because they will not show up in the final table
    search.aggs["projects"].metric("good_core_hours", "sum", field="GoodCpuCoreHours")

    totals.aggs.metric("cpu_core_hours", "sum", field="CpuCoreHours")
    totals.aggs.metric("good_core_hours", "sum", field="GoodCpuCoreHours")

    # calculate percentage within ES
    ROWS_AGGS.append(Aggregation(
                    get_percent_bucket_script("good_core_hours", "cpu_core_hours"),
                    "goodput_percent",
                    "% Goodput",
                    "metric",
    ))

    # convert lastremotewallclocktime to a double
    CAST_LRWT_SCRIPT_SRC = """
        double lrwt = 0;
        if (doc.containsKey("lastremotewallclocktime.keyword") && doc["lastremotewallclocktime.keyword"].size() > 0) {
            lrwt = Double.parseDouble(doc["lastremotewallclocktime.keyword"].value);
        }
        else if(doc.containsKey("RecordTime") && doc["RecordTime"].size() > 0
                && doc.containsKey("JobCurrentStartDate") && doc["JobCurrentStartDate"].size() > 0) {
            lrwt = doc["RecordTime"].value - doc["JobCurrentStartDate"].value;
        }

        emit(lrwt);
    """

    add_runtime_script(search, "LastRemoteWallClockTime", CAST_LRWT_SCRIPT_SRC, "double")
    add_runtime_script(totals, "LastRemoteWallClockTime", CAST_LRWT_SCRIPT_SRC, "double")

    # count total job unit hours
    # definition of 1 job unit - interpolated into painless script
    JOB_UNIT_DEF = {
        "cpus" : 1,
        "mem"  : 4096,      #mb
        "disk" : 4096*1024, #TODO assuming this is kb in ES?
    }

    JOB_UNIT_HOURS_SCRIPT_SRC = f"""
        double unit_hours = 0;
        if(doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0 
            && doc.containsKey("RequestMemory") && doc["RequestMemory"].size() > 0 
            && doc.containsKey("RequestDisk") && doc["RequestDisk"].size() > 0 
            && doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {{
            
            double units = Collections.max([
                Math.max(1, (int)doc["RequestCpus"].value) / {JOB_UNIT_DEF['cpus']},
                Math.max(0, (int)doc["RequestMemory"].value) / {JOB_UNIT_DEF['mem']},
                Math.max(0, (int)doc["RequestDisk"].value) / {JOB_UNIT_DEF['disk']}
            ]);

            unit_hours = ((double)doc["RemoteWallClockTime"].value / (double)3600) * units;  
        }}
        emit(unit_hours);
    """

    add_runtime_script(search, "jobUnitHours", JOB_UNIT_HOURS_SCRIPT_SRC, "double")
    add_runtime_script(totals, "jobUnitHours", JOB_UNIT_HOURS_SCRIPT_SRC, "double")

    ROWS_AGGS.append(Aggregation(
                    A("sum", field="jobUnitHours"),
                    "job_unit_hours",
                    "Job Unit Hours",
                    "metric",
    ))

    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # get percent checkpointable jobs

    # nested match query read as:
    # job must match JobUniverse=5 AND (WhenToTransferOutput=ON_EXIT_OR_EVICT AND Is_resumable=False)
    # OR (SuccessCheckpointExitBySignal=False AND SuccessCheckpointExitCode exists)
    # can this be reduced using python '&' and '|' ?
    cond_1 = Q("bool", must=[
                Q("match", WhenToTransferOutput__keyword="ON_EXIT_OR_EVICT"),
                Q("match", Is_resumable=False),
            ])

    cond_2 = Q("bool", must=[
                Q("match", SuccessCheckpointExitBySignal=False),
                Q("exists", field="SuccessCheckpointExitCode"),
            ])

    q = Q("bool", must=[
            Q("match", JobUniverse=5),
            Q("bool", should=[cond_1, cond_2]),
    ])

    # applied directly to the query again
    search.aggs["projects"].metric("ckptable_filt", "filter", filter=q)
    totals.aggs.metric("ckptable_filt", "filter", filter=q)

    # calculate percentage within ES
    ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                buckets_path={"num_ckptable" : "ckptable_filt._count",
                                "num_job_ids"  : "total_jobs"},
                script="params.num_ckptable / params.num_job_ids * 100"                 
                ),
                "ckptable_percent",
                "% Ckptable",
                "pipeline",
    ))

    # get percent rm'd jobs
    rmd_jobs = Q("match", JobStatus=3)
    search.aggs["projects"].metric("rmd_filt", "filter", filter=rmd_jobs)
    totals.aggs.metric("rmd_filt", "filter", filter=rmd_jobs)

    # calculate percentage within ES
    # pipeline aggregation doesn't create a new bucket
    # which appears to take up a lot of memory :)
    ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                buckets_path={"num_rmd" : "rmd_filt._count",
                                "num_job_ids"  : "total_jobs"},
                script="params.num_rmd / params.num_job_ids * 100"                 
                ),
                "rmd_percent",
                "% Removed",
                "pipeline",
    ))

    ## calculate shadow starts / job id
    search.aggs["projects"].metric("num_shadw_starts", "sum", field="NumShadowStarts")
    totals.aggs.metric("num_shadw_starts", "sum", field="NumShadowStarts")

    ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                buckets_path={"num_ss" : "num_shadw_starts",
                                "num_job_ids"  : "total_jobs"},
                script="params.num_ss / params.num_job_ids"                 
                ),
                "shadw_starts_per_id",
                "Shadow Starts / ID",
                "metric",
    ))

    ## calculate exec attempts / shadow start
    search.aggs["projects"].metric("num_exec_attempts", "sum", field="NumJobStarts")
    totals.aggs.metric("num_exec_attempts", "sum", field="NumJobStarts")

    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"num_ea" : "num_exec_attempts",
                                    "num_ss"  : "num_shadw_starts"},
                    script="params.num_ea / params.num_ss"                 
                    ),
                    "exec_att_per_shadw_start",
                    "Exec Att / Shadow Start",
                    "metric"
    ))

    # calculate holds / job id
    # numholds is a text field, need to cast to int
    CAST_HOLDS_SCRIPT_SRC = """
    if(doc.containsKey("numholds.keyword") && doc["numholds.keyword"].size() > 0) {
        emit(Double.parseDouble(doc["numholds.keyword"].value)); 
    }
    """
    add_runtime_script(search, "numHolds", CAST_HOLDS_SCRIPT_SRC, "double")
    add_runtime_script(totals, "numHolds", CAST_HOLDS_SCRIPT_SRC, "double")

    search.aggs["projects"].metric("num_holds", "sum", field="numHolds")
    totals.aggs.metric("num_holds", "sum", field="numHolds")

    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"holds" : "num_holds",
                                    "num_job_ids"  : "total_jobs"},
                    script="params.holds / params.num_job_ids"                 
                    ),
                    "hold_per_id",
                    "Holds / ID",
                    "metric"
    ))

    # calculate percentiles
    percentiles = [25.0, 50.0, 75.0, 95.0]
    ROWS_AGGS.append(Aggregation(A("percentiles", field="CpuCoreHours"), 
                                "percentiles", 
                                [f"{p}% Hrs" for p in percentiles], 
                                "metric",
                                [str(p) for p in percentiles] # percentiles agg returns keys "25.0", "50.0", ...
                    ))
    TOTALS_AGGS.append(ROWS_AGGS[-1])

    
    # calculate % short jobs
    short_job_filt = Q("bool", must=[
        Q("range", LastRemoteWallClockTime={"lt" : 60}),
        Q("range", LastRemoteWallClockTime={"gt" : 0}),
    ])
 
    search.aggs["projects"].metric("short_jobs", "filter", filter=short_job_filt)
    totals.aggs.metric("short_jobs", "filter", filter=short_job_filt)


    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"num_short" : "short_jobs._count",
                                    "num_job_ids"  : "total_jobs"},
                    script="params.num_short / params.num_job_ids * 100"
                    ),
                    "percent_short_jobs",
                    "% Short",
                    "pipeline",
    ))

    ## compute % of jobs with > 1 exec attempt
    restarted_filt = Q("range", NumJobStarts={"gt" : 1})
    search.aggs["projects"].metric("restarted_jobs", "filter", filter=restarted_filt)
    totals.aggs.metric("restarted_jobs", "filter", filter=restarted_filt)

    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"num_restarted" : "restarted_jobs._count",
                                    "num_job_ids"  : "total_jobs"},
                    script="params.num_restarted / params.num_job_ids * 100"
                    ),
                    "percent_restarted",
                    "% Restarted",
                    "pipeline",
    ))

    # computer % of jobs with > 1 hold
    one_hold_filt = Q("range", numholds={"gt" : 0})
    search.aggs["projects"].metric("held_jobs", "filter", filter=one_hold_filt)
    totals.aggs.metric("held_jobs", "filter", filter=one_hold_filt)

    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"num_held" : "held_jobs._count",
                                    "num_job_ids"  : "total_jobs"},
                    script="params.num_held / params.num_job_ids * 100"
                    ),
                    "percent_held",
                    "% Held",
                    "pipeline",
    ))

    # compute % jobs over requested disk
    over_disk_script = { "source" : "doc[\"DiskUsage\"].value > doc[\"RequestDisk\"].value" }
    over_disk_filt = Q("bool", filter=[Q("script", script=over_disk_script)])

    search.aggs["projects"].metric("over_disk_jobs", "filter", filter=over_disk_filt)
    totals.aggs.metric("over_disk_jobs", "filter", filter=over_disk_filt)

    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"num_over_disk" : "over_disk_jobs._count",
                                    "num_job_ids"  : "total_jobs"},
                    script="params.num_over_disk / params.num_job_ids * 100"
                    ),
                    "percent_over_disk",
                    "% Over Req. Disk",
                    "pipeline",
    ))

    # compute % of jobs using singularity
    sty_filt = Q("bool", filter=[Q("exists", field="SingularityImage")])
    search.aggs["projects"].metric("sty_jobs", "filter", filter=sty_filt)
    totals.aggs.metric("sty_jobs", "filter", filter=sty_filt)

    ROWS_AGGS.append(Aggregation(
                    A("bucket_script",
                    buckets_path={"num_sty_jobs" : "sty_jobs._count",
                                    "num_job_ids"  : "total_jobs"},
                    script="params.num_sty_jobs / params.num_job_ids * 100"
                    ),
                    "percent_sty",
                    "% S'ty Jobs",
                    "pipeline",
    ))

    # compute mean activation hours
    # need to cast activationduration to a double

    # TODO this seems off from sample report - numbers are too high
    CAST_ACTV_DURATION_SCRIPT_SRC = """
        if(doc.containsKey("activationduration.keyword") && doc["activationduration.keyword"].size() > 0) {
            emit(Double.parseDouble(doc["activationduration.keyword"].value) / (double)3600);
        }
    """

    add_runtime_script(search, "ActivationDuration", CAST_ACTV_DURATION_SCRIPT_SRC, "double")
    add_runtime_script(totals, "ActivationDuration", CAST_ACTV_DURATION_SCRIPT_SRC, "double")

    ROWS_AGGS.append(Aggregation(A("avg", field="ActivationDuration"), "mean_act_hrs", "Mean Actv Hours", "metric"))
    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # calculate percentiles
    percents = [25.0, 50.0, 75.0, 95.0]
    ROWS_AGGS.append(Aggregation(A("percentiles", field="CpuCoreHours", percents=percents), 
                                 "percentiles", 
                                 [f"{p}% Hrs" for p in percents], 
                                 "metric",
                                 [str(p) for p in percents]
                    ))
    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # calculate other stats
    ROWS_AGGS.append(Aggregation(A("extended_stats", field="CpuCoreHours"),
                                "stats",
                                ["Min Hrs", "Std Hrs", "Mean Hrs", "Max Hrs"],
                                "metric",
                                ["min", "avg", "std_deviation", "max"]))

    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # calculate mean setup secs
    # script to cast to double
    CAST_SETUP_DURATION_SCRIPT_SRC = """
        if(doc.containsKey("activationsetupduration.keyword") && doc["activationsetupduration.keyword"].size() > 0) {
            emit(Double.parseDouble(doc["activationsetupduration.keyword"].value));
        }
    """

    add_runtime_script(search, "SetupDuration", CAST_SETUP_DURATION_SCRIPT_SRC, "double")
    add_runtime_script(totals, "SetupDuration", CAST_SETUP_DURATION_SCRIPT_SRC, "double")

    ROWS_AGGS.append(Aggregation(A("avg", field="SetupDuration"), "mean_setup_secs", "Mean Setup Secs", "metric"))
    TOTALS_AGGS.append(ROWS_AGGS[-1])

    # =========== add aggregations to the two queries ==============

    for agg in ROWS_AGGS: 
        getattr(search.aggs["projects"], agg.type)(agg.name, agg.object)

    for agg in TOTALS_AGGS:
        getattr(totals.aggs, agg.type)(agg.name, agg.object)


    # =========== execute query and display results ===========

    # run the queries
    print(f"{datetime.now()} - Running query...")
    try:
        response = search.execute() 
        totals_response = totals.execute()
    except Exception as err:
        print(err.info)
        raise err

    # extract the final data
    table_rows = []
    for bucket in response.aggregations.projects.buckets:
        proj_name = bucket["key"]

        # extract data into a row
        # COL_AGG_NAMES defined at top of file
        row = {es_opts["es_agg_by"].split('.')[0] : proj_name}
        for agg in ROWS_AGGS:
            # check if it's a multi-value aggregation
            if isinstance(agg.pretty_name, list):
                for pretty_name, name in zip(agg.pretty_name, agg.mult_names):
                    try:
                        row.update({pretty_name : bucket[agg.name]["values"][name]})
                    except KeyError: 
                        row.update({pretty_name : bucket[agg.name][name]})
            else:
                row.update({agg.pretty_name : bucket[agg.name]["value"]})
        
        table_rows.append(row)

    # create the totals row
    totals_row = {es_opts["es_agg_by"].split('.')[0] : "Totals"}
    totals_raw = totals_response.aggregations.to_dict()

    for a in TOTALS_AGGS:
        # check if it's a multi-value aggregation
        if isinstance(a.pretty_name, list):
            for pretty_name, name in zip(a.pretty_name, a.mult_names):
                try:
                    totals_row.update({pretty_name : totals_raw[a.name]["values"][name]})
                except KeyError: 
                    totals_row.update({pretty_name : totals_raw[a.name][name]})
        else:
            totals_row.update({a.pretty_name : totals_raw[a.name]["value"]})

    final_totals = calc_totals_percents(totals_response) 
    totals_row.update(final_totals)
    table_rows.append(totals_row)

    # sort by # of job ids in descending order
    table_rows.sort(key=itemgetter("# Jobs"), reverse=True)

    # compute a table (for now)
    if args.print_table:
        print(f"Report for {args.start.strftime('%Y-%m-%d %H:%M:%S')} TO {args.end.strftime('%Y-%m-%d %H:%M:%S')}")
        table(table_rows)

    # send an email
    html = table(table_rows, emit_html=True) 

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
        {html}
    """

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
