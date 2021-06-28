import logging
import htcondor
import statistics as stats
from pathlib import Path
from .BaseFilter import BaseFilter


DEFAULT_COLUMNS = {
    10: "All CPU Hours",
    20: "% Good CPU Hours",
    40: "Num Uniq Job Ids",
    50: "% Rm'd Jobs",
    60: "% Short Jobs",
    
    70: "Shadw Starts / Job Id",
    80: "Exec Atts / Shadw Start",

    90: "% Jobs w/>1 Exec Att",

    110: "Min Hrs",
    120: "25% Hrs",
    130: "Med Hrs",
    140: "75% Hrs",
    145: "95% Hrs",
    150: "Max Hrs",
    160: "Mean Hrs",
    170: "Std Hrs",

    180: "Avg MB Sent",
    181: "Max MB Sent",
    190: "Avg MB Recv",
    191: "Max MB Recv",

    200: "Max Rqst Mem MB",
    210: "Med Used Mem MB",
    220: "Max Used Mem MB",
    230: "Max Rqst Cpus",

    300: "Good CPU Hours",
    305: "CPU Hours / Bad Exec Att",
    310: "Num Exec Atts",
    320: "Num Shadw Starts",
    330: "Num Rm'd Jobs",
    340: "Num DAG Node Jobs",
    350: "Num Jobs w/>1 Exec Att",
    360: "Num Short Jobs",
    370: "Num Local Univ Jobs",
    380: "Num Sched Univ Jobs",
}


class OsgScheddCpuMonthlyFilter(BaseFilter):
    name = "OSG schedd job history"
    
    def __init__(self, **kwargs):
        self.collector_host = "flock.opensciencegrid.org"
        self.schedd_collector_host_map = {}
        super().__init__(**kwargs)

    def schedd_collector_host(self, schedd):
        # Query Schedd ad in Collector for its CollectorHost,
        # unless result previously cached
        if schedd not in self.schedd_collector_host_map:

            collector = htcondor.Collector(self.collector_host)
            ads = collector.query(
                htcondor.AdTypes.Schedd,
                constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                projection=["CollectorHost"],
            )
            ads = list(ads)
            if len(ads) == 0:
                logging.warning(f'Could not find Schedd ClassAd for Machine == "{schedd}"')
                logging.warning(f"Assuming jobs from {schedd} are in OS pool")
                self.schedd_collector_host_map[schedd] = self.collector_host
                return self.collector_host
            if len(ads) > 1:
                logging.warning(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

            # Cache the CollectorHost in the map
            if "CollectorHost" in ads[0]:
                self.schedd_collector_host_map[schedd] = ads[0]["CollectorHost"].split(':')[0]
            else:
                logging.warning(f"CollectorHost not found in Schedd ClassAd for {schedd}")
                self.schedd_collector_host_map[schedd] = "UNKNOWN"

        return self.schedd_collector_host_map[schedd]

    def schedd_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        o = data["Schedds"][schedd]
        t = data["Schedds"]["TOTAL"]

        # Filter out jobs that did not run in the OS pool        
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        is_removed = i.get("JobStatus") == 3
        is_scheduler = i.get("JobUniverse") == 7
        is_local = i.get("JobUniverse") == 12
        has_shadow = not (is_scheduler or is_local)
        is_dagnode = i.get("DAGNodeName") is not None and i.get("JobUniverse", 5) != 12
        is_exec = i.get("NumJobStarts", 0) >= 1
        is_multiexec = i.get("NumJobStarts", 0) > 1
        is_short = False
        if has_shadow and not is_removed:
            if i.get("CommittedTime", 0) > 0 and i.get("CommittedTime", 60) < 60:
                is_short = True
            elif None in [i.get("RecordTime"), i.get("JobCurrentStartDate")]:
                if i.get("CommittedTime") == 0:
                    is_short = True
            elif i.get("RecordTime") - i.get("JobCurrentStartDate") < 60:
                is_short = True

        sum_cols = {}
        sum_cols["Jobs"] = 1
        sum_cols["RunJobs"] = int(is_exec)
        sum_cols["RmJobs"] = int(is_removed)
        sum_cols["SchedulerJobs"] = int(is_scheduler)
        sum_cols["LocalJobs"] = int(is_local)
        sum_cols["DAGNodeJobs"] = int(is_dagnode)
        sum_cols["MultiExecJobs"] = int(is_multiexec)
        sum_cols["ShortJobs"] = int(is_short)
        
        sum_cols["GoodCpuTime"] = (i.get("CommittedTime", 0) * i.get("RequestCpus", 1))
        sum_cols["CpuTime"] = (i.get("RemoteWallClockTime", 0) * i.get("RequestCpus", 1))
        sum_cols["BadCpuTime"] = ((i.get("RemoteWallClockTime", 0) - i.get("CommittedTime", 0)) * i.get("RequestCpus", 1))
        sum_cols["NumShadowStarts"] = int(is_hasshadow) * i.get("NumShadowStarts", 0)
        sum_cols["NumJobStarts"] = int(is_hasshadow) * i.get("NumJobStarts", 0)
        sum_cols["NumBadJobStarts"] = int(is_hasshadow) * max(i.get("NumJobStarts", 0) - 1, 0)
        sum_cols["BytesSent"] = int(is_exec) * i.get("BytesSent", 0)
        sum_cols["BytesRecvd"] = int(is_exec) * i.get("BytesRecvd", 0)

        max_cols = {}
        max_cols["MaxBytesSent"] = i.get("BytesSent", 0)
        max_cols["MaxBytesRecvd"] = i.get("BytesRecvd", 0)
        max_cols["MaxRequestMemory"] = i.get("RequestMemory", 0)
        sum_cols["MaxMemoryUsage"] = i.get("MemoryUsage", 0)
        sum_cols["MaxRequestCpus"] = i.get("RequestCpus", 1)

        list_cols = {}
        list_cols["MemoryUsage"] = i.get("MemoryUsage")
        list_cols["LongJobTimes"] = None
        if not is_short and not is_removed and has_shadow:
            list_cols["LongJobTimes"] = i.get("CommittedTime")

        set_cols = {}
        set_cols["User"] = i.get("User", "UNKNOWN")

        for col in sum_cols:
            o[col] = (o.get(col) or 0) + sum_cols[col]
            t[col] = (t.get(col) or 0) + sum_cols[col]
        for col in max_cols:
            o[col] = max((o.get(col) or 0), max_cols[col])
            t[col] = max((t.get(col) or 0) + max_cols[col])
        for col in list_cols:
            o[col].append(list_cols[col])
            t[col].append(list_cols[col])
        for col in set_cols:
            o[col] = (t.get(col) or set()).add(set_cols[col])
            t[col] = (t.get(col) or set()).add(set_cols[col])

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        o = data["Users"][user]

        # Filter out jobs that did not run in the OS pool
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["ScheddName", "ProjectName"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)
        
        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Do filtering for scheduler and local universe jobs
        univ = i.get("JobUniverse", 5)
        o["_NumSchedulerUnivJobs"].append(univ == 7)
        o["_NumLocalUnivJobs"].append(univ == 12)
        o["_NoShadow"].append(univ in [7, 12])

        # Compute badput fields
        if (
                univ not in [7, 12] and
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            # Use UNKNOWN for missing or blank ProjectName and ScheddName
            if attr in ["ScheddName", "ProjectName"]:
                o[attr].append(i.get(attr, "UNKNOWN") or "UNKNOWN")
            else:
                o[attr].append(i.get(attr, None))

    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this project
        project = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"
        o = data["Projects"][project]

        # Filter out jobs that did not run in the OS pool
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Do filtering for scheduler and local universe jobs
        univ = i.get("JobUniverse", 5)
        o["_NumSchedulerUnivJobs"].append(univ == 7)
        o["_NumLocalUnivJobs"].append(univ == 12)
        o["_NoShadow"].append(univ in [7, 12])

        # Compute badput fields
        if (
                univ not in [7, 12] and
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))

    
    def site_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that were removed
        if i.get("JobStatus", 4) == 3:
            return

        # Filter out scheduler and local universe jobs
        if i.get("JobUniverse") in [7, 12]:
            return

        # Get output dict for this site
        site = i.get("MachineAttrGLIDEIN_ResourceName0", "UNKNOWN") or "UNKNOWN"
        o = data["Site"][site]

        # Filter out jobs that did not run in the OS pool
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.schedd_filter,
            #self.user_filter,
            #self.project_filter,
            #self.site_filter,
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
            rm_columns = [20,50,70,80,90,300,305,310,320,330,340,350,370,380]
            [columns.pop(key) for key in rm_columns]
        return columns
            
    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        if agg == "Site":
            columns_sorted = list(rows[0])
            columns_sorted[columns_sorted.index("All CPU Hours")] = "Final Exec Att CPU Hours"
            rows[0] = tuple(columns_sorted)
        return rows


    def compute_site_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute goodput and total CPU hours columns
        goodput_cpu_time = []
        for (goodput_time, cpus) in zip(
                data["CommittedTime"],
                data["RequestCpus"]):
            if None in [goodput_time, cpus]:
                goodput_cpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)

        # Short jobs are jobs that ran for < 1 minute
        is_short_job = []
        for (goodput_time, record_date, start_date) in zip(
                data["CommittedTime"],
                data["RecordTime"],
                data["JobCurrentStartDate"]):
            if (goodput_time is not None) and (goodput_time > 0):
                is_short_job.append(goodput_time < 60)
            elif None in (record_date, start_date):
                is_short_job.append(None)
            else:
                is_short_job.append((record_date - start_date) < 60)

        # "Long" (i.e. "normal") jobs ran >= 1 minute
        # We only want to use these when computing percentiles,
        # so filter out short jobs and removed jobs,
        # and sort them so we can easily grab the percentiles later
        long_times_sorted = []
        for (is_short, goodput_time) in zip(
                is_short_job,
                data["CommittedTime"]):
            if (is_short == False):
                long_times_sorted.append(goodput_time)
        long_times_sorted = self.clean(long_times_sorted)
        long_times_sorted.sort()

        # Compute columns
        row["All CPU Hours"]   = sum(self.clean(goodput_cpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Avg MB Sent"]      = stats.mean(self.clean(data["BytesSent"], allow_empty_list=False)) / 1e6
        row["Max MB Sent"]      = max(self.clean(data["BytesSent"], allow_empty_list=False)) / 1e6
        row["Avg MB Recv"]      = stats.mean(self.clean(data["BytesRecvd"], allow_empty_list=False)) / 1e6
        row["Max MB Recv"]      = max(self.clean(data["BytesRecvd"], allow_empty_list=False)) / 1e6
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Num Users"] = len(set(data["User"]))
   
        if row["Num Uniq Job Ids"] > 0:
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["% Short Jobs"] = 0

        # Compute time percentiles and stats
        if len(long_times_sorted) > 0:
            row["Min Hrs"]  = long_times_sorted[ 0] / 3600
            row["25% Hrs"]  = long_times_sorted[  len(long_times_sorted)//4] / 3600
            row["Med Hrs"]  = stats.median(long_times_sorted) / 3600
            row["75% Hrs"]  = long_times_sorted[3*len(long_times_sorted)//4] / 3600
            row["95% Hrs"]  = long_times_sorted[int(0.95*len(long_times_sorted))] / 3600
            row["Max Hrs"]  = long_times_sorted[-1] / 3600
            row["Mean Hrs"] = stats.mean(long_times_sorted) / 3600
        else:
            for col in [f"{x} Hrs" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0

        if len(long_times_sorted) > 1:
            row["Std Hrs"] = stats.stdev(long_times_sorted) / 3600
        else:
            # There is no variance if there is only one value
            row["Std Hrs"] = 0

        # Compute mode for Project and Schedd columns in the Users table
        row["Num Users"] = len(set(data["User"]))

        return row

    def compute_custom_columns(self, data, agg, agg_name):

        if agg == "Site":
            row = self.compute_site_custom_columns(data, agg, agg_name)
            return row

        # Output dictionary
        row = {}

        # Compute columns
        row["All CPU Hours"]    = data["CpuTime"] / 3600
        row["Good CPU Hours"]   = data["GoodCpuTime"] / 3600
        row["Num Uniq Job Ids"] = data["Jobs"]
        row["Num DAG Node Jobs"] = data["DAGNodeJobs"]
        row["Num Rm'd Jobs"]    = data["RmJobs"]
        row["Num Jobs w/>1 Exec Att"] = data["MultiExecJobs"]
        row["Avg MB Sent"]      = (data["BytesSent"] / data["RunJobs"]) / 1e6
        row["Max MB Sent"]      = data["MaxBytesSent"] / 1e6
        row["Avg MB Recv"]      = (data["BytesRecvd"] / data["RunJobs"]) / 1e6
        row["Max MB Recv"]      = data["MaxBytesRecvd"] / 1e6
        row["Num Short Jobs"]   = data["ShortJobs"]
        row["Max Rqst Mem MB"]  = data["MaxRequestMemory"]
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = data["MaxMemoryUsage"]
        row["Max Rqst Cpus"]    = data["MaxRequestCpus"]
        row["Num Exec Atts"]    = data["NumJobStarts"]
        row["Num Shadw Starts"] = data["NumShadowStarts"]
        row["Num Local Univ Jobs"] = data["LocalJobs"]
        row["Num Sched Univ Jobs"] = data["SchedulerJobs"]

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
            projects = self.clean(data["ProjectName"])
            if len(projects) > 0:
                row["Most Used Project"] = max(set(projects), key=projects.count)
            else:
                row["Most Used Project"] = "UNKNOWN"

            schedds = self.clean(data["ScheddName"])
            if len(schedds) > 0:
                row["Most Used Schedd"] = max(set(schedds), key=schedds.count)
            else:
                row["Most Used Schedd"] = "UNKNOWN"
        if agg == "Projects":
            row["Num Users"] = len(set(data["User"]))  

        return row 

    def scan_and_filter(self, es_index, start_ts, end_ts, **kwargs):
        filtered_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        
        query = self.get_query(
            index=es_index,
            start_ts=start_ts,
            end_ts=end_ts,
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
