def get_args():
    import argparse

    parser = argparse.ArgumentParser(description="Parse LSF bhist output to CSV")
    parser.add_argument("--user", help="LSF username")
    parser.add_argument("--output", default="jobs_summary.csv", help="Output CSV file path")
    parser.add_argument("--max-workers", default=2, help="Maximum number of threads")

    return parser.parse_args()