
import htcondor
import pickle
import statistics as stats
from pathlib import Path
from collections import defaultdict
from operator import itemgetter
from elasticsearch import Elasticsearch
import elasticsearch.helpers
from functools import lru_cache
from .BaseFilter import BaseFilter
from accounting.pull_topology import get_site_map


DEFAULT_COLUMNS = {
    10: "All CPU Hours",
    20: "Num Uniq Job Ids",
    30: "% Good CPU Hours",

#    45: "% Ckpt Able",
    50: "% Rm'd Jobs",
    60: "% Short Jobs",
    70: "% Jobs w/>1 Exec Att",
    80: "% Jobs w/1+ Holds",
    83: "Total Files Xferd",

    85: "Shadw Starts / Job Id",
    90: "Exec Atts / Shadw Start",
    95: "Holds / Job Id",

    110: "Min Hrs",
    120: "25% Hrs",
    130: "Med Hrs",
    140: "75% Hrs",
    145: "95% Hrs",
    150: "Max Hrs",
    160: "Mean Hrs",
    170: "Std Hrs",

    180: "Input Files / Exec Att",
#    181: "Input MB / Exec Att",
#    182: "Input MB / File",
    190: "Output Files / Job",
#    191: "Output MB / Job",
#    192: "Output MB / File",

    200: "Max Rqst Mem MB",
    #210: "Med Used Mem MB",
    220: "Max Used Mem MB",
    230: "Max Rqst Cpus",

    300: "Good CPU Hours",
    305: "CPU Hours / Bad Exec Att",
    310: "Num Exec Atts",
    320: "Num Shadw Starts",
    325: "Num Job Holds",
    330: "Num Rm'd Jobs",
    340: "Num DAG Node Jobs",
    350: "Num Jobs w/>1 Exec Att",
    355: "Num Jobs w/1+ Holds",
    360: "Num Short Jobs",
    370: "Num Local Univ Jobs",
    380: "Num Sched Univ Jobs",
#    390: "Num Ckpt Able Jobs",
}


SITE_MAP = get_site_map()


class OsgScheddCpuMonthlyFilter(BaseFilter):
    name = "OSG schedd job history"

    def __init__(self, **kwargs):
        self.collector_hosts = {"cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"}
        self.schedd_collector_host_map_pickle = Path("ospool-host-map.pkl")
        self.schedd_collector_host_map = {}
        if self.schedd_collector_host_map_pickle.exists():
            try:
                self.schedd_collector_host_map = pickle.load(open(self.schedd_collector_host_map_pickle, "rb"))
            except IOError:
                pass
        super().__init__(**kwargs)

    def schedd_collector_host(self, schedd):
        # Query Schedd ad in Collector for its CollectorHost,
        # unless result previously cached
        if schedd not in self.schedd_collector_host_map:
            self.logger.debug(f"Schedd {schedd} not found in cached collector host map, querying collector")
            self.schedd_collector_host_map[schedd] = set()

            for collector_host in self.collector_hosts:
                if collector_host in {"flock.opensciencegrid.org"}:
                    continue
                collector = htcondor.Collector(collector_host)
                ads = collector.query(
                    htcondor.AdTypes.Schedd,
                    constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                    projection=["CollectorHost"],
                )
                ads = list(ads)
                if len(ads) == 0:
                    continue
                if len(ads) > 1:
                    self.logger.warning(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

                # Cache the CollectorHost in the map
                if "CollectorHost" in ads[0]:
                    schedd_collector_hosts = set()
                    for schedd_collector_host in ads[0]["CollectorHost"].split(","):
                        schedd_collector_host = schedd_collector_host.strip().split(":")[0]
                        if schedd_collector_host:
                            schedd_collector_hosts.add(schedd_collector_host)
                    if schedd_collector_hosts:
                        self.schedd_collector_host_map[schedd] = schedd_collector_hosts
                        break
            else:
                self.logger.warning(f"Did not find Machine == {schedd} in collectors")

        return self.schedd_collector_host_map[schedd]

    @lru_cache(maxsize=250)
    def is_ospool_job(self, schedd_name, last_remote_pool):
        remote_pool = set()
        if last_remote_pool is not None:
            if last_remote_pool.strip():
                return last_remote_pool in self.collector_hosts
        if schedd_name is not None:
            if schedd_name.strip():
                remote_pools = self.schedd_collector_host(schedd_name)
                return bool(remote_pools & self.collector_hosts)
        return False

    def reduce_data(self, i, o, t):

        is_removed = i.get("JobStatus") == 3
        is_scheduler = i.get("JobUniverse") == 7
        is_local = i.get("JobUniverse") == 12
        has_shadow = not (is_scheduler or is_local)
        is_dagnode = i.get("DAGNodeName") is not None and i.get("JobUniverse", 5) != 12
        is_exec = i.get("NumJobStarts", 0) >= 1
        is_multiexec = i.get("NumJobStarts", 0) > 1
        has_holds = i.get("NumHolds", 0) > 0
        is_short = False
        goodput_time = 0
        if has_shadow and not is_removed:
            goodput_time = i.get("LastRemoteWallClockTime", i.get("CommittedTime", 0))
            if goodput_time > 0 and goodput_time < 60:
                is_short = True
            elif None in [i.get("RecordTime"), i.get("JobCurrentStartDate")]:
                if goodput_time == 0:
                    is_short = True
            elif i.get("RecordTime") - i.get("JobCurrentStartDate") < 60:
                is_short = True
        elif not is_removed:
            goodput_time = i.get("LastRemoteWallClockTime", i.get("CommittedTime", 0))
        input_files = 0
        output_files = 0
        if has_shadow:
            input_file_stats = i.get("TransferInputStats", {})
            output_file_stats = i.get("TransferOutputStats", {})
            for key, value in input_file_stats.items():
                if key.casefold().endswith("FilesCountTotal".casefold()):
                    input_files += value
            for key, value in output_file_stats.items():
                if key.casefold().endswith("FilesCountTotal".casefold()):
                    output_files += value

        sum_cols = {}
        sum_cols["Jobs"] = 1
        sum_cols["RunJobs"] = int(is_exec)
        sum_cols["RmJobs"] = int(is_removed)
        sum_cols["SchedulerJobs"] = int(is_scheduler)
        sum_cols["LocalJobs"] = int(is_local)
        sum_cols["DAGNodeJobs"] = int(is_dagnode)
        sum_cols["MultiExecJobs"] = int(is_multiexec)
        sum_cols["ShortJobs"] = int(is_short)
        sum_cols["ShadowJobs"] = int(has_shadow)

        sum_cols["GoodCpuTime"] = (goodput_time * i.get("RequestCpus", 1))
        sum_cols["CpuTime"] = (i.get("RemoteWallClockTime", 0) * i.get("RequestCpus", 1))
        sum_cols["BadCpuTime"] = ((i.get("RemoteWallClockTime", 0) - goodput_time) * i.get("RequestCpus", 1))
        sum_cols["NumShadowStarts"] = int(has_shadow) * i.get("NumShadowStarts", 0)
        sum_cols["NumJobStarts"] = int(has_shadow) * i.get("NumJobStarts", 0)
        sum_cols["NumBadJobStarts"] = int(has_shadow) * max(i.get("NumJobStarts", 0) - 1, 0)
        sum_cols["HeldJobs"] = int(has_holds)
        sum_cols["NumJobHolds"] = i.get("NumHolds", 0)
        if input_files > 0:
            sum_cols["InputFiles"] = input_files
        if output_files > 0:
            sum_cols["OutputFiles"] = output_files
        if input_files > 0 or output_files > 0:
            sum_cols["TotalFiles"] = input_files + output_files

        max_cols = {}
        max_cols["MaxRequestMemory"] = i.get("RequestMemory", 0)
        max_cols["MaxMemoryUsage"] = i.get("MemoryUsage", 0)
        max_cols["MaxRequestCpus"] = i.get("RequestCpus", 1)

        list_cols = {}
        #list_cols["MemoryUsage"] = i.get("MemoryUsage")
        list_cols["LongJobTimes"] = None
        if not is_short and not is_removed and has_shadow:
            list_cols["LongJobTimes"] = goodput_time

        for col in sum_cols:
            o[col] = (o.get(col) or 0) + sum_cols[col]
            t[col] = (t.get(col) or 0) + sum_cols[col]
        for col in max_cols:
            o[col] = max([(o.get(col) or 0), max_cols[col]])
            t[col] = max([(t.get(col) or 0), max_cols[col]])
        for col in list_cols:
            o[col].append(list_cols[col])
            t[col].append(list_cols[col])

    def schedd_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i.get("ScheddName"), i.get("LastRemotePool")):
            return

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        output = data["Schedds"][schedd]
        total = data["Schedds"]["TOTAL"]

        self.reduce_data(i, output, total)

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i.get("ScheddName"), i.get("LastRemotePool")):
            return

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        output = data["Users"][user]
        total = data["Users"]["TOTAL"]

        self.reduce_data(i, output, total)

        counter_cols = {}
        counter_cols["ScheddNames"] = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        counter_cols["ProjectNames"] = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"

        for col in counter_cols:
            if not col in output:
                output[col] = defaultdict(int)
            output[col][counter_cols[col]] += 1
            if not col in total:
                total[col] = defaultdict(int)
            total[col][counter_cols[col]] += 1

    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i.get("ScheddName"), i.get("LastRemotePool")):
            return

        # Get output dict for this project
        project = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"
        output = data["Projects"][project]
        total = data["Projects"]["TOTAL"]

        self.reduce_data(i, output, total)

        dict_cols = {}
        dict_cols["Users"] = i.get("User", "UNKNOWN") or "UNKNOWN"

        for col in dict_cols:
            output[col] = output.get(col) or {}
            output[col][dict_cols[col]] = 1
            total[col] = total.get(col) or {}
            total[col][dict_cols[col]] = 1

    def site_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i.get("ScheddName"), i.get("LastRemotePool")):
            return

        # Filter out jobs that were removed
        if i.get("JobStatus", 4) == 3:
            return

        # Filter out scheduler and local universe jobs
        if i.get("JobUniverse") in [7, 12]:
            return

        # Get output dict for this site
        site = i.get("MachineAttrGLIDEIN_ResourceName0")
        if (site is None) or (not site):
            site = "Unknown (resource name missing)"
        else:
            site = SITE_MAP.get(site, f"Unmapped resource: {site}")
        o = data["Site"][site]
        t = data["Site"]["TOTAL"]

        # Reduce data
        is_short = False
        if i.get("CommittedTime", 0) > 0 and i.get("CommittedTime", 60) < 60:
            is_short = True
        elif None in [i.get("RecordTime"), i.get("JobCurrentStartDate")]:
            if i.get("CommittedTime") == 0:
                is_short = True
        elif i.get("RecordTime") - i.get("JobCurrentStartDate") < 60:
            is_short = True

        sum_cols = {}
        sum_cols["Jobs"] = 1
        sum_cols["ShortJobs"] = int(is_short)

        sum_cols["GoodCpuTime"] = (i.get("CommittedTime", 0) * i.get("RequestCpus", 1))

        max_cols = {}
        max_cols["MaxRequestMemory"] = i.get("RequestMemory", 0)
        max_cols["MaxMemoryUsage"] = i.get("MemoryUsage", 0)
        max_cols["MaxRequestCpus"] = i.get("RequestCpus", 1)

        list_cols = {}
        #list_cols["MemoryUsage"] = i.get("MemoryUsage")
        list_cols["LongJobTimes"] = None
        if not is_short:
            list_cols["LongJobTimes"] = i.get("CommittedTime")

        dict_cols = {}
        dict_cols["Users"] = i.get("User", "UNKNOWN") or "UNKNOWN"

        for col in sum_cols:
            o[col] = (o.get(col) or 0) + sum_cols[col]
            t[col] = (t.get(col) or 0) + sum_cols[col]
        for col in max_cols:
            o[col] = max([(o.get(col) or 0), max_cols[col]])
            t[col] = max([(t.get(col) or 0), max_cols[col]])
        for col in list_cols:
            o[col].append(list_cols[col])
            t[col].append(list_cols[col])
        for col in dict_cols:
            o[col] = o.get(col) or {}
            o[col][dict_cols[col]] = 1
            t[col] = t.get(col) or {}
            t[col][dict_cols[col]] = 1

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.schedd_filter,
            self.user_filter,
            self.project_filter,
            self.site_filter,
        ]
        return filters

    def add_custom_columns(self, agg):
        # Add Project and Schedd columns to the Users table
        columns = DEFAULT_COLUMNS.copy()
        if agg == "Users":
            columns[5] = "Most Used Project"
            columns[175] = "Most Used Schedd"
        if agg == "Projects":
            columns[5] = "Num Users"
        if agg == "Site":
            columns[5] = "Num Users"
            rm_columns = [30,50,70,80,83,85,90,95,180,190,300,305,310,320,325,330,340,350,355,370,380]
            [columns.pop(key) for key in rm_columns]
        return columns

    def compute_site_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute columns
        row["All CPU Hours"]    = data["GoodCpuTime"] / 3600
        row["Num Uniq Job Ids"] = data["Jobs"]
        row["Num Short Jobs"]   = data["ShortJobs"]
        row["Max Rqst Mem MB"]  = data["MaxRequestMemory"]
        #row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = data["MaxMemoryUsage"]
        row["Max Rqst Cpus"]    = data["MaxRequestCpus"]
        row["Num Users"]        = len(data["Users"])

        if row["Num Uniq Job Ids"] > 0:
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["% Short Jobs"] = 0

        # Compute time percentiles and stats
        data["LongJobTimes"] = self.clean(data["LongJobTimes"])
        n = len(data["LongJobTimes"])
        if n > 0:
            data["LongJobTimes"].sort()

            row["Min Hrs"]  = data["LongJobTimes"][0] / 3600
            row["25% Hrs"]  = data["LongJobTimes"][n//4] / 3600
            row["Med Hrs"]  = stats.median(data["LongJobTimes"]) / 3600
            row["75% Hrs"]  = data["LongJobTimes"][(3*n)//4] / 3600
            row["95% Hrs"]  = data["LongJobTimes"][int(0.95*n)] / 3600
            row["Max Hrs"]  = data["LongJobTimes"][-1] / 3600
            row["Mean Hrs"] = stats.mean(data["LongJobTimes"]) / 3600
        else:
            for col in [f"{x} Hrs" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if n > 1:
            row["Std Hrs"] = stats.stdev(data["LongJobTimes"]) / 3600
        else:
            # There is no variance if there is only one value
            row["Std Hrs"] = 0

        return row

    def compute_custom_columns(self, data, agg, agg_name):

        if agg == "Site":
            row = self.compute_site_custom_columns(data, agg, agg_name)
            return row

        # Output dictionary
        row = {}

        # Compute columns
        row["All CPU Hours"]     = data["CpuTime"] / 3600
        row["Num Uniq Job Ids"]  = data["Jobs"]
        row["Good CPU Hours"]    = data["GoodCpuTime"] / 3600
        row["Num DAG Node Jobs"] = data["DAGNodeJobs"]
        row["Num Rm'd Jobs"]     = data["RmJobs"]
        row["Num Jobs w/>1 Exec Att"] = data["MultiExecJobs"]
        row["Num Jobs w/1+ Holds"] = data["HeldJobs"]
        row["Total Files Xferd"] = data.get("TotalFiles", "")

        row["Num Short Jobs"]   = data["ShortJobs"]
        row["Max Rqst Mem MB"]  = data["MaxRequestMemory"]
        #row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = data["MaxMemoryUsage"]
        row["Max Rqst Cpus"]    = data["MaxRequestCpus"]
        row["Num Exec Atts"]    = data["NumJobStarts"]
        row["Num Shadw Starts"] = data["NumShadowStarts"]
        row["Num Job Holds"]    = data["NumJobHolds"]
        row["Num Local Univ Jobs"] = data["LocalJobs"]
        row["Num Sched Univ Jobs"] = data["SchedulerJobs"]

        if data["NumJobStarts"] > 0 and data.get("InputFiles") is not None:
            row["Input Files / Exec Att"] = data["InputFiles"] / data["NumJobStarts"]
        else:
            row["Input Files / Exec Att"] = ""
        if data["ShadowJobs"] > 0 and data.get("OutputFiles") is not None:
            row["Output Files / Job"] = data["OutputFiles"] / data["ShadowJobs"]
        else:
            row["Output Files / Job"] = ""

        # Compute derivative columns
        if row["All CPU Hours"] > 0:
            row["% Good CPU Hours"] = 100 * row["Good CPU Hours"] / row["All CPU Hours"]
        else:
            row["% Good CPU Hours"] = 0
        if row["Num Uniq Job Ids"] > 0:
            row["Shadw Starts / Job Id"] = row["Num Shadw Starts"] / row["Num Uniq Job Ids"]
            row["% Rm'd Jobs"] = 100 * row["Num Rm'd Jobs"] / row["Num Uniq Job Ids"]
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs w/>1 Exec Att"] = 100 * row["Num Jobs w/>1 Exec Att"] / row["Num Uniq Job Ids"]
            row["% Jobs w/1+ Holds"] = 100 * row["Num Jobs w/1+ Holds"] / row["Num Uniq Job Ids"]
            row["Holds / Job Id"] = row["Num Job Holds"] / row["Num Uniq Job Ids"]
        else:
            row["Shadw Starts / Job Id"] = 0
            row["% Rm'd Jobs"] = 0
            row["% Short Jobs"] = 0
            row["% Jobs w/>1 Exec Att"] = 0
        if row["Num Shadw Starts"] > 0:
            row["Exec Atts / Shadw Start"] = row["Num Exec Atts"] / row["Num Shadw Starts"]
        else:
            row["Exec Atts / Shadw Start"] = 0
        if data["NumBadJobStarts"] > 0:
            row["CPU Hours / Bad Exec Att"] = (data["BadCpuTime"] / data["NumBadJobStarts"]) / 3600
        else:
            row["CPU Hours / Bad Exec Att"] = 0

        # Compute time percentiles and stats
        data["LongJobTimes"] = self.clean(data["LongJobTimes"])
        n = len(data["LongJobTimes"])
        if n > 0:
            data["LongJobTimes"].sort()

            row["Min Hrs"]  = data["LongJobTimes"][0] / 3600
            row["25% Hrs"]  = data["LongJobTimes"][n//4] / 3600
            row["Med Hrs"]  = stats.median(data["LongJobTimes"]) / 3600
            row["75% Hrs"]  = data["LongJobTimes"][(3*n)//4] / 3600
            row["95% Hrs"]  = data["LongJobTimes"][int(0.95*n)] / 3600
            row["Max Hrs"]  = data["LongJobTimes"][-1] / 3600
            row["Mean Hrs"] = stats.mean(data["LongJobTimes"]) / 3600
        else:
            for col in [f"{x} Hrs" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if n > 1:
            row["Std Hrs"] = stats.stdev(data["LongJobTimes"]) / 3600
        else:
            # There is no variance if there is only one value
            row["Std Hrs"] = 0

        # Compute mode for Project and Schedd columns in the Users table
        if agg == "Users":
            projects = data["ProjectNames"]
            if len(projects) > 0:
                row["Most Used Project"] = max(projects.items(), key=itemgetter(1))[0]
            else:
                row["Most Used Project"] = "UNKNOWN"

            schedds = data["ScheddNames"]
            if len(schedds) > 0:
                row["Most Used Schedd"] = max(schedds.items(), key=itemgetter(1))[0]
            else:
                row["Most Used Schedd"] = "UNKNOWN"
        if agg == "Projects":
            row["Num Users"] = len(data["Users"])

        return row

    def scan_and_filter(self, es_index, start_ts, end_ts, **kwargs):
        filtered_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        query = self.get_query(
            index=es_index,
            start_ts=start_ts,
            end_ts=end_ts,
            scroll="10s",
            size=600,
        )

        # Use the scan() helper function, which automatically scrolls results. Nice!
        for doc in elasticsearch.helpers.scan(
                client=self.client,
                query=query.pop("body"),
                **query,
                ):

            # Send the doc through the various filters,
            # which mutate filtered_data in place
            for filtr in self.get_filters():
                filtr(filtered_data, doc)

        return filtered_data

    def merge_filtered_data(self, data, agg):
        # Takes filtered data and an aggregation level (e.g. Users, Schedds,
        # Projects) and returns a list of tuples, with the first item
        # containing a tuple of column names in the order that matches
        # the following tuples of values

        # Get the dict of columns
        columns = self.add_custom_columns(agg)

        # Make the 0th column the aggregation level
        # e.g. agg on "Users"   -> "User" column
        #      agg on "Schedds" -> "Schedd" column
        columns[0] = agg.rstrip("s")

        # Get the names of the columns in order
        columns_sorted = [col for (n, col) in sorted(columns.items())]

        # Loop over aggregated data and store computed data in rows
        rows = []
        for agg_name, d in data[agg].items():
            row = {}

            # It's possible to have no job data stored
            # if the dict was initialized but then job data was skipped
            if d.get("Jobs", 0) == 0:
                continue

            # Put the name for this aggregation
            # (e.g. user name, schedd name, project name)
            # as the value for the 0th column
            row[columns[0]] = agg_name

            # Compute any custom columns
            row.update(self.compute_custom_columns(d, agg, agg_name))

            # Store a tuple of all column data in order
            rows.append(tuple(row[col] for col in columns_sorted))

        # Sort rows by All CPU Hours
        rows.sort(reverse=True, key=itemgetter(columns_sorted.index("All CPU Hours")))

        # Prepend the header row
        rows.insert(0, tuple(columns_sorted))

        if agg == "Site":
            columns_sorted = list(rows[0])
            columns_sorted[columns_sorted.index("All CPU Hours")] = "Final Exec Att CPU Hours"
            rows[0] = tuple(columns_sorted)

        return rows
