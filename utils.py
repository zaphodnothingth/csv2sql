from sqlalchemy import engine_from_config
import configparser
import sys
from datetime import datetime

OUTFILE = sys.argv[0] + ".log"
ord_list = list(range(48,57,1)) + list(range(65,90,1)) + [95] + list(range(97,122,1))


def write_msg(msg):
    if OUTFILE:
        f = open(OUTFILE, "a")
        f.write(msg)
        f.flush()
    sys.stdout.write(msg)
    sys.stdout.flush()


def write_err(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()


def print_msg(msg):
    msg = "\n{} - {}".format(datetime.now().strftime('%Y%m%d_%H%M%S'), msg)
    if OUTFILE:
        open(OUTFILE, "a").write(msg+"\n")
    print(msg)


def set_db_engines():
    parse_cfg = configparser.ConfigParser()
    parse_cfg.read('oracle_db.cfg')

    schema_engine = engine_from_config(parse_cfg['db_connect_schema'])
    schema_engine = engine_from_config(parse_cfg['db_connect_schema'])
    schema_engine = engine_from_config(parse_cfg['db_connect_schema'])

    return {
        "schema": schema_engine,
        "schema": schema_engine,
        "schema": schema_engine
    }
