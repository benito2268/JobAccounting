import sys
from datetime import datetime, timedelta
import logging
import csv
import smtplib
import time
import pickle
import json
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from urllib.error import HTTPError
from pathlib import Path
from math import ceil
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
from dns.resolver import query as dns_query


TOPOLOGY_PROJECT_DATA_URL = "https://topology.opensciencegrid.org/miscproject/xml"
TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"
INSTITUTION_IDS_DATA_URL = "https://topology-institutions.osg-htc.org/api/institution_ids"


def get_timestamps(report_period, start_ts, end_ts):
    if report_period == "custom" and None in (start_ts, end_ts):
        raise ValueError("START_TS and END_TS cannot be None when REPORT_PERIOD is custom")
    if report_period != "custom" and None not in (start_ts, end_ts):
        raise ValueError("REPORT_PERIOD must be custom if both START_TS and END_TS defined")

    if report_period == "custom":
        return (start_ts, end_ts)

    if start_ts is not None:
        sign = 1
        ts0 = start_ts
    elif end_ts is not None:
        sign = -1
        ts0 = end_ts
    else:  # Default ending timestamp to midnight today
        end_dt = datetime.now()
        end_dt = datetime(end_dt.year, end_dt.month, end_dt.day)
        sign = -1
        ts0 = end_dt.timestamp()

    if report_period == "daily":
        ts1 = ts0 + sign*timedelta(days=1).total_seconds()
    elif report_period == "weekly":
        ts1 = ts0 + sign*timedelta(days=7).total_seconds()
    elif report_period == "monthly":
        dt0 = datetime.fromtimestamp(ts0)
        try:
            dt1 = dt0.replace(month=dt0.month + sign)
        except ValueError:
            if sign > 0:
                dt1 = dt0.replace(year=dt0.year+1, month=1)
            else:
                dt1 = dt0.replace(year=dt0.year-1, month=12)
        ts1 = dt1.timestamp()
    else:
        raise ValueError("REPORT_PERIOD is not one of [daily, weekly, monthly]")

    return tuple(sorted([int(ts0), int(ts1)]))


def write_csv(table, filter_name, table_name, start_ts, report_period, csv_dir, **kwargs):
    # Write a CSV to file named:
    #   1. Filter name (e.g. job history)
    #   2. Table name (e.g. Users, Schedds, Projects)
    #   3. Date (Starting + Reporting Period or Starting + Ending)
    filter_name = filter_name.replace(" ", "-")
    table_name = table_name.replace(" ", "-")

    # Format date(s)
    start = datetime.fromtimestamp(start_ts)
    if report_period in ["daily", "weekly", "monthly"]:
        start_date = start.strftime("%Y-%m-%d")
        date = f"{report_period}_{start_date}"
    else:
        end = datetime.fromtimestamp(kwargs["end_ts"])
        start_date = start.strftime("%Y%m%d%H%M%S")
        end_date = end.strftime("%Y%m%d%H%M%S")
        date = f"{start_date}_{end_date}"

    # Set filename and path
    filename = f"{filter_name}_{table_name}_{date}.csv"
    filepath = Path(csv_dir) / filename

    # Write CSV
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        for row in table:
            # Cap floats at 1e-4 precision
            writer.writerow([f"{x:.4f}" if isinstance(x, float) else x for x in row])

    # Return path to CSV
    return filepath


def _smtp_mail(msg, recipient, smtp_server=None, smtp_username=None, smtp_password=None):
    logger = logging.getLogger("accounting.send_email")
    sent = False
    result = None
    tries = 0
    sleeptime = 0
    while tries < 3 and sleeptime < 600:
        try:
            logger.debug(f"Connecting to mailserver {smtp_server}")
            if smtp_username is None:
                smtp = smtplib.SMTP(smtp_server)
            else:
                smtp = smtplib.SMTP_SSL(smtp_server)
                smtp.login(smtp_username, smtp_password)
        except Exception:
            logger.error(f"Could not connect to {smtp_server}")
            continue

        try:
            logger.debug(f"Sending email to {recipient}")
            result = smtp.sendmail(msg["From"], recipient, msg.as_string())
            if len(result) > 0:
                logger.error(f"Could not send email to {recipient} using {smtp_server}:\n{result}")
            else:
                sent = True
        except Exception:
            logger.exception(f"Could not send to {recipient} using {smtp_server}")
        finally:
            try:
                smtp.quit()
            except smtplib.SMTPServerDisconnected:
                pass
        if sent:
            break

        sleeptime = int(min(30 * 1.5**tries, 600))
        logger.info(f"Sleeping for {sleeptime} seconds before retrying servers")
        time.sleep(sleeptime)
        tries += 1

    else:
        logger.error(f"Failed to send email after {tries} loops")

    return sent


def send_email(subject, from_addr, to_addrs, cc_addrs, bcc_addrs, reply_to_addr, html,
               table_files=[], smtp_server=None, smtp_username=None, smtp_password_file=None, **kwargs):
    logger = logging.getLogger("accounting.send_email")
    if len(to_addrs) == 0:
        logger.error("No recipients in the To: field, not sending email")
        print("ERROR: No recipients in the To: field, not sending email", file=sys.stderr)
        return

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if len(cc_addrs) > 0:
        msg["Cc"] = ", ".join(cc_addrs)
    if len(bcc_addrs) > 0:
        msg["Bcc"] = ", ".join(bcc_addrs)
    if reply_to_addr is not None:
        msg["Reply-To"] = reply_to_addr
    msg["Subject"] = subject
    msg["Message-ID"] = f"<{subject}-{time.time()}-{from_addr}>".replace(" ", "-").casefold()
    msg["Date"] = formatdate(localtime=True)

    msg.attach(MIMEText(html, "html"))

    for fname in table_files:
        fpath = Path(fname)
        part = MIMEBase("application", "octet-stream")
        with fpath.open("rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename = fpath.name)
        msg.attach(part)

    if smtp_server is not None:
        recipient = list(set(to_addrs + cc_addrs + bcc_addrs))
        smtp_password = None
        if smtp_password_file is not None:
            smtp_password = smtp_password_file.open("r").read().strip()
        _smtp_mail(msg, recipient, smtp_server, smtp_username, smtp_password)

    else:
        for recipient in set(to_addrs + cc_addrs + bcc_addrs):
            domain = recipient.split("@")[1]
            sent = False
            for mxi, mx in enumerate(dns_query(domain, "MX")):
                smtp_server = str(mx).split()[1][:-1]

                try:
                    sent = _smtp_mail(msg, recipient, smtp_server)
                except Exception:
                    continue
                if sent:
                    break

                sleeptime = int(min(30 * 1.5**mxi, 600))
                logger.info(f"Sleeping for {sleeptime} seconds before trying next server")
                time.sleep(sleeptime)

            else:
                logger.error("Failed to send email after trying all servers")


def get_job_units(cpus, memory_gb, disk_gb):
    unit_size = {
        "cpus": 1,
        "memory_gb": 4,
        "disk_gb": 4,
    }
    job_units = max([
        max(1, cpus/unit_size["cpus"]),
        max(0, memory_gb/unit_size["memory_gb"]),
        max(0, disk_gb/unit_size["disk_gb"])
    ])
    return ceil(job_units)


def get_topology_project_data(
        force_update=False,
        project_data_path=Path("topology_project_data_map.pickle"),
    ) -> dict:
    if (
        not force_update and
        project_data_path.is_file() and
        project_data_path.stat().st_mtime > (time.time() - 24*3600)
    ):
        projects_map = pickle.load(project_data_path.open("rb"))
    else:
        tries = 0
        max_tries = 5
        while tries < max_tries:
            try:
                with urlopen(TOPOLOGY_PROJECT_DATA_URL) as xml:
                    xmltree = ET.parse(xml)
            except HTTPError:
                time.sleep(2**tries)
                tries += 1
                if tries == max_tries:
                    raise
            else:
                break
        projects = xmltree.getroot()
        projects_map = {
            "UNKNOWN": {
                "name": "UNKNOWN",
                "pi": "UNKNOWN",
                "pi_institution": "UNKNOWN",
                "field_of_science": "UNKNOWN",
            }
        }

        for project in projects:
            project_map = {}
            project_map["name"] = project.find("Name").text
            project_map["pi"] = project.find("PIName").text
            project_map["pi_institution"] = project.find("Organization").text
            project_map["field_of_science"] = project.find("FieldOfScience").text
            project_map["id"] = project.find("ID").text
            project_map["pi_institution_id"] = project.find("InstitutionID").text
            project_map["field_of_science_id"] = project.find("FieldOfScienceID").text
            projects_map[project_map["name"].lower()] = project_map.copy()

        pickle.dump(projects_map, project_data_path.open("wb"))

    return projects_map


def get_topology_resource_data(
        force_update=False,
        resource_data_path=Path("topology_resource_data_map.pickle"),
    ) -> dict:
    if (
        not force_update and
        resource_data_path.is_file() and
        resource_data_path.stat().st_mtime > (time.time() - 24*3600)
    ):
        resources_map = pickle.load(resource_data_path.open("rb"))
    else:
        tries = 0
        max_tries = 5
        while tries < max_tries:
            try:
                with urlopen(TOPOLOGY_RESOURCE_DATA_URL) as xml:
                    xmltree = ET.parse(xml)
            except HTTPError:
                time.sleep(2**tries)
                tries += 1
                if tries == max_tries:
                    raise
            else:
                break
        resource_groups = xmltree.getroot()
        resources_map = {
            "UNKNOWN": {
                "name": "UNKNOWN",
                "institution": "UNKNOWN",
            }
        }

        for resource_group in resource_groups:
            resource_institution = resource_group.find("Facility").find("Name").text
            resource_institution_id = resource_group.find("Facility").find("ID").text
            resource_institution_osg_id = resource_group.find("Facility").find("InstitutionID").text

            resources = resource_group.find("Resources")
            for resource in resources:
                resource_map = {}
                resource_map["institution"] = resource_institution
                resource_map["institution_id"] = resource_institution_id
                resource_map["osg_id"] = resource_institution_osg_id
                resource_map["name"] = resource.find("Name").text
                resource_map["id"] = resource.find("ID").text
                resources_map[resource_map["name"].lower()] = resource_map.copy()

        pickle.dump(resources_map, resource_data_path.open("wb"))

    return resources_map


def get_prp_mapping_data(
        force_update=False,
        prp_data_path=Path("prp_data_map.pickle"),
    ) -> dict:
    if (
        not force_update and
        prp_data_path.is_file() and
        prp_data_path.stat().st_mtime > (time.time() - 24*3600)
    ):
        prp_id_map = pickle.load(prp_data_path.open("rb"))
    else:
        prp_id_map = {}
        tries = 0
        max_tries = 5
        while tries < max_tries:
            try:
                with urlopen(INSTITUTION_IDS_DATA_URL) as f:
                    for resource in json.load(f):
                        institution = resource["name"]
                        osg_id_short = (resource.get("id") or "").split("/")[-1]
                        ror_id_short = (resource.get("ror_id") or "").split("/")[-1]
                        if osg_id_short:
                            prp_id_map[osg_id_short] = institution
                        # OSG_INSTITUTION_IDS mistakenly had the ROR IDs before ~2024-11-07,
                        # so we map those too (as long as they don't conflict with OSG IDs)
                        if ror_id_short and ror_id_short not in prp_id_map:
                            prp_id_map[ror_id_short] = institution
            except HTTPError:
                time.sleep(2**tries)
                tries += 1
                if tries == max_tries:
                    raise
            else:
                break

        pickle.dump(prp_id_map, prp_data_path.open("wb"))

    return prp_id_map
