import subprocess
import re
from typing import List

def get_args():
    import argparse

    parser = argparse.ArgumentParser(description="Parse LSF bhist output to CSV")
    parser.add_argument("--user", help="LSF username")
    parser.add_argument("--output", default="jobs_summary.csv", help="Output CSV file path")

    return parser.parse_args()

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
            job_ids.append(int(job_id))

    return job_ids

def parse_bjobs_details(output: str) -> List[str]:
    """
    Given a job id return the row in a dictionary of a
    parser version of bjobs -l <job_id> output
    """

    row = [
        re.search(r"Job <(\d+)>", output).group(1),
        re.search(r"User <(.*?)>", output).group(1),
        re.search(r"Status <(.*?)>", output).group(1),
        re.search(r"Queue <(.*?)>", output).group(1),
        float(re.search(r"rusage\[mem=(\d+\.?\d*)\]", output).group(1)) if re.search(r"rusage\[mem=(\d+\.?\d*)\]", output) else "",
        float(re.search(r"The CPU time used is ([\d.]+) seconds", output).group(1)) if re.search(r"The CPU time used is ([\d.]+) seconds", output) else "",
        int(re.search(r"MEM: (\d+) Mbytes", output).group(1)) if re.search(r"MEM: (\d+) Mbytes", output) else "",
        int(re.search(r"SWAP: (\d+) Mbytes", output).group(1)) if re.search(r"SWAP: (\d+) Mbytes", output) else "",
        int(re.search(r"NTHREAD: (\d+)", output).group(1)) if re.search(r"NTHREAD: (\d+)", output) else "",
        re.search(r"(\w+ \w+ +\d+ \d+:\d+:\d+): Started", output).group(1) if re.search(r"(\w+ \w+ +\d+ \d+:\d+:\d+): Started", output) else "",
        re.search(r"(\w+ \w+ +\d+ \d+:\d+:\d+): Resource usage collected", output).group(1) if re.search(r"(\w+ \w+ +\d+ \d+:\d+:\d+): Resource usage collected", output) else "",
        re.search(r"Started .*? on Host\(s\) <(.*?)>", output).group(1) if re.search(r"Started .*? on Host\(s\) <(.*?)>", output) else ""
    ]

    return row

from exporter import Exporter

def export(headers: List[str], rows: List, exporter: Exporter, output: str = "jobs.csv"):
    exporter.export(
        headers=headers, 
        data=rows, 
        path=output
    )


if __name__ == "__main__":
    args = get_args()

    import subprocess

    bjobs_output = subprocess.check_output(
        ["bjobs", "-noheader", "-a"] if not args.user else ["bjobs", "-noheader", "-a", "-u", args.user],
        text=True
    )

    job_ids = get_job_ids(bjobs_output)


    headers = ["JobID", "User", "Status", "Queue", "Rusage", "CPUTime", "MEM", "SWAP", "NTHREAD", "Started", "Finished", "Node"]
    rows = []


    for job_id in job_ids:

        bjobs = subprocess.run(
            ["bjobs", "-l", str(job_id)],
            stdout=subprocess.PIPE,
            text=True
        )

        output = bjobs.stdout

        rows.append(parse_bjobs_details(output))

    from exporter import CSVExporter
    export(headers, rows, CSVExporter(), args.output)

