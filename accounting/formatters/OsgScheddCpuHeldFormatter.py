from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict
from string import ascii_uppercase


HOLD_REASONS = [
    "Unspecified",
    "UserRequest",
    "GlobusGramError",
    "JobPolicy",
    "CorruptedCredential",
    "JobPolicyUndefined",
    "FailedToCreateProcess",
    "UnableToOpenOutput",
    "UnableToOpenInput",
    "UnableToOpenOutputStream",
    "UnableToOpenInputStream",
    "InvalidTransferAck",
    "DownloadFileError",
    "UploadFileError",
    "IwdError",
    "SubmittedOnHold",
    "SpoolingInput",
    "JobShadowMismatch",
    "InvalidTransferGoAhead",
    "HookPrepareJobFailure",
    "MissedDeferredExecutionTime",
    "StartdHeldJob",
    "UnableToInitUserLog",
    "FailedToAccessUserAccount",
    "NoCompatibleShadow",
    "InvalidCronSettings",
    "SystemPolicy",
    "SystemPolicyUndefined",
    "GlexecChownSandboxToUser",
    "PrivsepChownSandboxToUser",
    "GlexecChownSandboxToCondor",
    "PrivsepChownSandboxToCondor",
    "MaxTransferInputSizeExceeded",
    "MaxTransferOutputSizeExceeded",
    "JobOutOfResources",
    "InvalidDockerImage",
    "FailedToCheckpoint",
    "EC2UserError",
    "EC2InternalError",
    "EC2AdminError",
    "EC2ConnectionProblem",
    "EC2ServerError",
    "EC2InstancePotentiallyLostError",
    "PreScriptFailed",
    "PostScriptFailed",
    "SingularityTestFailed",
    "JobDurationExceeded",
    "JobExecuteExceeded",
]

def hhmm(hours):
    # Convert float hours to HH:MM
    h = int(hours)
    m = int(60 * (float(hours) - int(hours)))
    return f"{h:02d}:{m:02d}"

def break_camel(s):
    # Break before [A-Z]
    zero_width_space = "&#8203;"
    for char in ascii_uppercase:
        s = s.replace(char, f"{zero_width_space}{char}")
    return s


class OsgScheddCpuHeldFormatter(BaseFormatter):

    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for once held jobs completed on <strong>{start_date}</strong>"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for once held jobs completed from <strong>{start_date} to {end_date}</strong>"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for once held jobs completed from <strong>{start_date} to {end_date}</strong>"
        return title_str

    def get_subject(self, report_period, start_ts, end_ts, **kwargs):
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily", "weekly", "monthly"]:
            start_date = start.strftime("%Y-%m-%d")
            subject_str = f"OSPool {report_period.capitalize()} Held Jobs Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool Held Jobs Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {
            "Good CPU Hours",
            "Num Exec Atts",
            "Num Shadw Starts",
            "Num Job Holds",
            "Num Rm'd Jobs",
            "Num Jobs w/>1 Exec Att",
            "Num Short Jobs",
        }
        num_holds_icols = [i for i, header in enumerate(data["header"]) if header[:11] == "% Holds for"]
        for i in num_holds_icols:
            if float(data["rows"][0][i]) < 0.0000001:
                cols.add(data["header"][i])
            else:
                data["header"][i] = break_camel(data["header"][i])
        return super().rm_cols(data, cols=cols)

    def get_table_html(self, table_file, report_period, start_ts, end_ts, **kwargs):
        return super().get_table_html(table_file, report_period, start_ts, end_ts, **kwargs)

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        custom_fmts = {
            "Most Common Hold Reason": lambda x: f'<td class="text">{break_camel(x)}</td>',
            "Min Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "25% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Med Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "75% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "95% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Max Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Mean Hrs":   lambda x: f"<td>{hhmm(x)}</td>",
            "Std Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "CPU Hours / Bad Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "Shadw Starts / Job Id":    lambda x: f"<td>{float(x):.2f}</td>",
            "Exec Atts / Shadw Start":  lambda x: f"<td>{float(x):.3f}</td>",
            "Holds / Job Id":           lambda x: f"<td>{float(x):.2f}</td>",
            "% Holds Most Comm Reas": lambda x: f"<td>{float(x):.1f}</td>",
            "% Rm'd Jobs":            lambda x: f"<td>{float(x):.1f}</td>",
            "% Short Jobs":           lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs w/>1 Exec Att":   lambda x: f"<td>{float(x):.1f}</td>",
        }
        for reason in HOLD_REASONS:
            custom_fmts[break_camel(f"% Holds for {reason}")] = lambda x: f"<td>{float(x):.1f}</td>"
        return super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)

    def get_legend(self):
        custom_items = OrderedDict()
        custom_items["Num Uniq Job Ids"] = "Number of unique job ids across all execution attempts"
        custom_items["All CPU Hours"]    = "Total CPU hours for all execution attempts, including preemption and removal"
        custom_items["% Good CPU Hours"] = "Good CPU Hours per All CPU Hours, as a percentage"
        custom_items["Good CPU Hours"]   = "Total CPU hours for execution attempts that ran to completion"
        custom_items["Max Rqst Mem MB"]  = "Maximum memory requested across all submitted jobs in MB"
        custom_items["Max Used Mem MB"]  = "Maximum measured memory usage across all submitted jobs' last execution attempts in MB"
        custom_items["Max Rqst Cpus"]    = "Maximum number of CPUs requested across all submitted jobs"

        custom_items["Num Shadw Starts"] = "Total times a condor_shadow was spawned across all submitted jobs (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Exec Atts"]    = "Total number of execution attempts (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Rm'd Jobs"]    = "Number of jobs that were removed from the queue instead of allowing to complete"
        custom_items["Num Short Jobs"]   = "Number of execution attempts that completed in less than 60 seconds"
        custom_items["Num Jobs w/>1 Exec Att"] = "Number of unique jobs that were executed more than once"

        custom_items["% Rm'd Jobs"] = "Percent of Num Uniq Job Ids that were removed"
        custom_items["% Short Jobs"] = "Percent of Num Uniq Job Ids that were short jobs"
        custom_items["% Jobs w/>1 Exec Att"] = "Percent of Num Uniq Job Ids that had more than one execution attempt"

        custom_items["Shadw Starts / Job Id"]   = "Num Shadw Starts per Num Uniq Job Ids"
        custom_items["Exec Atts / Shadw Start"] = "Num Exec Atts per Num Shadw Starts"
        custom_items["Holds / Job Id"] = "Num Job Holds per Num Uniq Job Ids"

        custom_items["Min/25%/Median/75%/Max/Mean/Std Hrs"] = "Final execution wallclock hours that a non-short job (Min-Max) or jobs (Mean/Std) ran for (excluding Short jobs, excluding Local and Scheduler Universe jobs)"

        custom_items["Avg MB Sent"] = "Mean MB sent to a job sandbox from a submit point"
        custom_items["Avg MB Recv"] = "Mean MB sent to a submit point from a job sandbox"

        custom_items["Med Used Mem MB"]  = "Median measured memory usage across all submitted jobs' last execution attempts in MB"

        custom_items["CPU Hours / Bad Exec Att"] = "Average CPU Hours used in a non-final execution attempt"
        html = super().get_legend(custom_items)
        return html

