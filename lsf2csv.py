#! /usr/bin/env python3

import subprocess
import re
from typing import List
from concurrent.futures import ThreadPoolExecutor
from logger import setup_logger
import logging

logger = setup_logger(__name__, level=logging.INFO)

headers = ["JobID", "User", "Status", "Queue", "CPUTime", "MaxMEM", "Node","AVGMEM", "Started", "Finished"]

RE_JOB_ID = r"Job <(\d+)>"
RE_USER = r"User <(.*?)>"
RE_STATUS = r"Status <(.*?)>"
RE_QUEUE = r"Queue <(.*?)>"
RE_CPU_TIME = r"The CPU time used is ([\d.]+) seconds"
RE_MAX_MEM = r"MAX MEM: (\d+) Mbytes"
RE_AVG_MEM = r"AVG MEM: (\d+) Mbytes"
RE_NODE = r"Started .*? on Host\(s\) <(.*?)>"
RE_STARTED = r"(\w+ \w+ +\d+ \d+:\d+:\d+): Started"
RE_FINISHED = r"(\w+ \w+ +\d+ \d+:\d+:\d+): [(Resource usage collected)(Done successfully)]"

def get_job_ids(bjobs_output: str) -> List[int]:
    """
    Get job ids from bjobs -a output extracting the first column,
    which represents the job id.
    
    Args:
        bjobs_output (str): bjobs -a output
    """

    jobs = [line.strip() for line in bjobs_output.splitlines()]

    job_ids = []

    for line in jobs:
        # extract first column
        job_id = line.split(' ')[0]

        # ensure job ids are valid (filter cores or empty values)
        if job_id != '' and job_id.isnumeric():
            job_ids.append(job_id)

    return job_ids

def parse_bjobs_details(output: str) -> List[str]:
    """
    Given the output of bjobs -l <job_id>, this function returns
    a list representing the row values
    """

    # remove any sequence of whitespaces (\n, \t or multiple spaces)
    output = re.sub(r"\s+", " ", output).strip()

    row = [
        re.search(RE_JOB_ID, output).group(1),
        re.search(RE_USER, output).group(1),
        re.search(RE_STATUS, output).group(1),
        re.search(RE_QUEUE, output).group(1),
        float(re.search(RE_CPU_TIME, output).group(1)) if re.search(RE_CPU_TIME, output) else "",
        int(re.search(RE_MAX_MEM, output).group(1)) if re.search(RE_MAX_MEM, output) else "",
        int(re.search(RE_AVG_MEM, output).group(1)) if re.search(RE_AVG_MEM, output) else "",
        re.search(RE_NODE, output).group(1) if re.search(RE_NODE, output) else "",
    ]

    row = [str(value).replace(' ', '') for value in row]

    # add datetimes avoiding removing whitespace
    row.extend([
        re.search(RE_STARTED, output).group(1) if re.search(RE_STARTED, output) else "",
        re.search(RE_FINISHED, output).group(1) if re.search(RE_FINISHED, output) else "",
    ])

    return row

def fetch_and_parse(job_id: str) -> List[str]:
    """
    Run bjobs -l for a job_id and parse the output
    """
    result = subprocess.run(
        ["bjobs", "-l", job_id],
        stdout=subprocess.PIPE,
        text=True
    )
    output = result.stdout
    return parse_bjobs_details(output)

def fetch_all_jobs_concurrent(job_ids: List[str], max_workers: int = 4) -> List[List[str]]:
    """
    Fetch and parse multiple jobs concurrently
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(fetch_and_parse, job_ids)
    return list(results)

from exporter import Exporter

def export(headers: List[str], rows: List, exporter: Exporter, output: str = "jobs.csv"):
    exporter.export(
        headers=headers, 
        data=rows, 
        path=output
    )

    logger.info(f"The jobs have been exported to: {output} successfully")

if __name__ == "__main__":
    import parser
    args = parser.get_args()

    import subprocess
    # -noheader exclude headers row, -a all jobs
    cmd = ["bjobs", "-noheader", "-a"]

    if args.user:
        cmd.extend(["-u", args.user])

    bjobs_output = subprocess.check_output(
        cmd,
        text=True
    )

    logger.info("Getting job ids")
    job_ids = get_job_ids(bjobs_output)

    logger.info("Extracting info from bjobs")
    rows = fetch_all_jobs_concurrent(job_ids, max_workers=args.max_workers)

    if len(rows) == 0:
        logger.warning("No jobs were found")
        exit(1)

    from exporter import CSVExporter
    logger.info("Exporting")
    export(headers, rows, CSVExporter(), args.output)
