"""todo
1 for append mode, need to check if incoming columns agree with target table (currently it'll just crash)
"""


import pandas as pd
import argparse
import time
from datetime import datetime, timedelta
import sys
import re
from sqlalchemy.types import String
import json

# project
import utils

engines = utils.set_db_engines()


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='transform CSV to oracle database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-files', dest="filenames", default=[], nargs="+" )
    parser.add_argument('-table', dest="table", default='py_imp_{}'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))
    parser.add_argument('-mode', default='new')
    parser.add_argument('-db', default='dev')
    parser.add_argument('-sep', default=',')

    return parser.parse_args(args)


def parse_mode(inp_mode):
    if inp_mode == 'new':
        to_sql_mode = 'fail'
    if inp_mode == 'replace':
        to_sql_mode = 'replace'
    if inp_mode == 'append':
        to_sql_mode = 'append'

    return to_sql_mode


def main(args):
    start_time = time.time()
    pargs = parse_args(args)
    to_sql_mode = parse_mode(pargs.mode)

    filenames = pargs.filenames
    utils.print_msg("Transforming files:\n\t{} \nto table:\n\t{}".format(filenames, pargs.table))

    file_count = 0
    total_files = len(filenames)
    inp_cols = []

    for filename in filenames:
        dtype_dict = dict()
        file_count += 1
        read_df = pd.read_csv(r"{}".format(filename), sep=pargs.sep, encoding='cp1252')

        # setup column list & df
        if file_count == 1:
            inp_cols = list(read_df.columns.values)
            input_df = read_df

        # make sure each new file has no new columns - missing columns are acceptable
        else:
            utils.write_msg("\n verifying no new columns in {} of {}: {}".format(file_count, total_files, filename))
            for col in read_df.columns:
                if col not in inp_cols:
                    utils.write_err("*** column {} is not in original - breaking ***".format(col))
                    break
            utils.write_msg("\n\t appending {} rows".format(read_df.shape[0]))
            input_df = input_df.append(read_df, ignore_index=True)

    # cleaning unicode out of entire dataframe
    input_df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
    # cleaning column names to make the oracle gods happy, but retain underscores first
    input_df.columns = input_df.columns.str.strip()
    input_df.columns = input_df.columns.str.replace(' ', '_')
    input_df.columns = input_df.columns.str.lower()
    for j in range(len(input_df.columns.values)):
        input_df.columns.values[j] = "".join(i for i in input_df.columns.values[j] if ord(i) in utils.ord_list)
    # remove possible duplicate column names since we pulled junk out
    input_df.columns = pd.io.parsers.ParserBase({'names':input_df.columns})._maybe_dedup_names(input_df.columns)
    # these are reserved oracle words
    input_df.rename(columns={'type':'type_', 'group':'group_', 'date': 'date_', 'resource':'resource_',
                           'start':'start_', 'end':'end_'}, inplace=True)

    # try to convert any columns with 'dt' or 'date' in name or with a regex date in [0] to datetime
    utils.write_msg("attempting to fix dates")
    for col in input_df.columns:
        if any([piece for piece in ['dt', 'date'] if piece in col.lower()])\
                or re.match('(\d{1,4})[^0-9a-zA-Z](\d{1,4})[^0-9a-zA-Z](\d{1,4})', str(input_df[col][0])):
            input_df[col] = pd.to_datetime(input_df[col], infer_datetime_format=True, errors='coerce')
            utils.write_msg("Attempted to correct {} to datetime - did it work? {}\n"
                  .format(col, input_df[col].dtype.kind == 'M'))  # 'M' is numpy dtype for datetime
        # deal with the fact that to_sql, sqlalchemy and oracle aren't friends - this chgs dtype to varchar ilo clob
        if input_df[col].dtype == 'object':
            dtype_dict[col] = String(input_df[col].apply(str).map(len).max())

    utils.print_msg("list of columns: \n{}".format(input_df.dtypes))
    try:
        utils.print_msg("\nlist of string conversions: \n{}".format(json.dumps(dtype_dict, indent=2)))
    except:
        utils.print_msg("\nlist of string conversions: \n{}".format(dtype_dict))
    utils.print_msg("\ndf length: \n{}".format(input_df.shape[0]))
    # input_df = input_df.where((pd.notnull(input_df)), None)
    utils.print_msg("\ndf head: \n{}".format(input_df.head()))
    input_df.to_sql(pargs.table, engines[pargs.db], if_exists=to_sql_mode, dtype=dtype_dict, index=False, chunksize=10)

    seco = int(time.time() - start_time)
    utils.print_msg('finished in {}'.format(timedelta(seconds=seco)))


if __name__ == "__main__":
    utils.print_msg("begin\n")
    main(sys.argv[1:])
