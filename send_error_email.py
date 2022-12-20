import accounting
import sys
from datetime import datetime
from collections import deque


def get_name(filter):
    filter_name = filter.__name__
    name = ""
    if 'Osg' in filter_name:
        name = "OSPool"
    if "Chtc" in filter_name:
        name = "CHTC"
    if "Gpu" in filter_name:
        name = f"{name} GPU"
    return name


def get_subject(args):
    datestr = datetime.fromtimestamp(args.start_ts).strftime("%Y-%m-%d")
    subject_str = f"Error sending {get_name(args.filter)} {args.report_period.capitalize()} Usage Report {datestr}"
    return subject_str


def log_tail(logfile, n=10):
    lines = deque(maxlen=n)
    with open(logfile, "r") as f:
        for line in f:
            lines.append(line.rstrip())
    return "\n".join(lines)


if __name__ == "__main__":
    args = accounting.parse_args(sys.argv[1:])
    subject = get_subject(args)
    html = f"""
<html>
<head>
</head>
<body>
Last 10 lines of log
<p style="font-family: monospace; white-space: pre">
{log_tail(args.log_file)}
</p>
</body>
</html>
"""
    accounting.send_email(
        subject=subject,
        html=html,
        **vars(args),
    )

