#!/usr/bin/env python3

""" This file implements conversion from simulation output to the format
acceptable by dashboard.

To be used independently, the following objects need to be transferred:
    1. Constants (class)
    2. convert_table (function)

After that, calling convert with pandas DataFrame in the source format would
return pandas DataFrame in the target format.

A way to use with copying this file, is to do import:
    from convert import convert_table

Usage from command line:
    unix:
        python3 convert.py path_src_table.csv path_output_table.csv
    windows:
        python3.exe convert.py path_src_table.csv path_output_table.csv

"""

# Author: Aliaksandr Nekrashevich
# aliaksandr.nekrashevich@queensu.ca
# (c) Smith School of Business, 2020


import argparse
import os
from typing import Dict, List, Type

import pandas as pd


class Constants:
    """ Contains all constant fields needed for conversion. """

    class SourceColumns:
        """ Names and patterns for source table columns. """

        AVG_CONSTRUCTION_SIZE = "average_construction_size_per_month"
        AVG_NONCOMPLIANCE = "average_noncompliance_detection_per_month"
        AVG_CLAIMS = "average_claims_per_month"
        AVG_RATIO = "average_claim_to_noncompliance_ratio"
        AVG_NONCOMPLIANCE_PERCENT = "noncompliance_percent"

        PATTERN_CONSTRUCTION_SIZE = "construction_size_month_{}"
        PATTERN_NONCOMPLIANCE = "noncompliance_month_{}"
        PATTERN_CLAIMS = "claims_month_{}"

        MONTHS_IN_YEAR = 12
        RANGE_MONTHS: List[int] = list(range(1, MONTHS_IN_YEAR + 1))

    class DestinationColumns:
        """ Names and patterns for destination table columns. """

        NAME: str = 'Name'
        MONTH: str = 'Month'
        WORKERS: str = 'Workers'
        NONCOMPLIANCE: str = 'non-compliant_detected'
        CLAIMS: str = 'claims'
        AVG_CLAIMS: str = 'avg_claim_per_month'
        AVG_WORKERS: str = 'avg_workers_per_month'
        AVG_NONCOMPLIANCE: str = 'avg_non_compliant_detection_per_month'
        AVG_NONCOMPLIANCE_PERCENT: str = 'avg_non_compliant_worker_ratio'
        AVG_RATIO: str = 'avg_claim_avg_non_compliant_detected_ratio'

    class DestinationKeys:
        """ Names and patterns for destination table entries. """
        PATTERN_COMPANY_NAME = 'Company {}'


def convert_table(src_table: pd.DataFrame) -> pd.DataFrame:
    """ Converts table obtained by simulation to the format
    acceptble by the dashboard.

    Arguments:
        src_table: DataFrame created by simulation code.

    Returns:
        DataFrame that can be used by the dashboard.
    """

    SourceColumns: Type[Constants.SourceColumns] = Constants.SourceColumns
    DestinationColumns: Type[
            Constants.DestinationColumns] = Constants.DestinationColumns
    DestinationKeys: Type[
            Constants.DestinationKeys] = Constants.DestinationKeys

    # This dictionary will be converted to the returned DataFrame.
    dst_table_dict: Dict[str, List[object]] = {
        DestinationColumns.NAME: list(),
        DestinationColumns.MONTH: list(),
        DestinationColumns.WORKERS: list(),
        DestinationColumns.NONCOMPLIANCE: list(),
        DestinationColumns.CLAIMS: list(),
        DestinationColumns.AVG_CLAIMS: list(),
        DestinationColumns.AVG_WORKERS: list(),
        DestinationColumns.AVG_NONCOMPLIANCE: list(),
        DestinationColumns.AVG_NONCOMPLIANCE_PERCENT: list(),
        DestinationColumns.AVG_RATIO: list()
    }

    index: int
    data: Dict[str, object]
    for index, data in src_table.iterrows():
        company_name = DestinationKeys.PATTERN_COMPANY_NAME.format(index)
        month_idx: int
        for month_idx in SourceColumns.RANGE_MONTHS:
            dst_table_dict[DestinationColumns.NAME].append(company_name)
            dst_table_dict[DestinationColumns.MONTH].append(month_idx)

            dst_table_dict[DestinationColumns.WORKERS].append(
                data[SourceColumns.PATTERN_CONSTRUCTION_SIZE.format(month_idx)])
            dst_table_dict[DestinationColumns.NONCOMPLIANCE].append(
                data[SourceColumns.PATTERN_NONCOMPLIANCE.format(month_idx)])
            dst_table_dict[DestinationColumns.CLAIMS].append(
                data[SourceColumns.PATTERN_CLAIMS.format(month_idx)])

            dst_table_dict[DestinationColumns.AVG_CLAIMS].append(
                data[SourceColumns.AVG_CLAIMS])
            dst_table_dict[DestinationColumns.AVG_WORKERS].append(
                data[SourceColumns.AVG_CONSTRUCTION_SIZE])
            dst_table_dict[DestinationColumns.AVG_NONCOMPLIANCE].append(
                data[SourceColumns.AVG_NONCOMPLIANCE])
            dst_table_dict[DestinationColumns.AVG_NONCOMPLIANCE_PERCENT].append(
                data[SourceColumns.AVG_NONCOMPLIANCE_PERCENT])
            dst_table_dict[DestinationColumns.AVG_RATIO].append(
                data[SourceColumns.AVG_RATIO])

    return pd.DataFrame.from_dict(dst_table_dict)


def main(args: argparse.Namespace) -> None:
    """ Main entry point if to be executed from the command line. """

    assert args.src.lower().endswith(".csv"), "Source table not in CSV!"
    assert os.path.isfile(args.src), "Source table does not exist!"
    assert args.dst.lower().endswith(".csv"), "Destination file extension not CSV!"

    src_table = pd.read_csv(args.src)
    dst_table = convert_table(src_table)

    dst_directory = os.path.dirname(args.dst)
    if not os.path.exists(dst_directory) and len(dst_directory) > 0:
        os.makedirs(dst_directory)

    dst_table.to_csv(args.dst, index=False)


def parse_arguments() -> argparse.Namespace:
    """ Auxiliary function for CLI argument collection. """
    parser = argparse.ArgumentParser(description=
        "Convert simulation tables to dashboard-friendly format.")
    parser.add_argument('src', type=str, help="Path to source table, in CSV format.")
    parser.add_argument('dst', type=str, help="Where to output destination table, in CSV format.")
    args = parser.parse_args()
    return args


# This ensures that the code would not be executed on import.
if __name__ == "__main__":
    args = parse_arguments()
    main(args)

