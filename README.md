# lsf2csv

Tool to convert `bjobs -l` output to CSV format for **all existing jobs** (`bjobs -a`).

You can **add more fields** to detect by adding your custom regex and the corresponding header.


Requirements:
- `python 3.10`

## How to use

Run `lsf2csv`

```bash
./lsf2csv
```

Optional arguments:

- `--user`: filter jobs by user
- `--output`: specify output CSV file

## Develop

```bash
conda env create -f environment.yml
conda activate lsf2csv
```
