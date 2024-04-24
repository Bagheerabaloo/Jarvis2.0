from src.kdb import KDB
from src.utils import print_exception, get_month_intervals
import pandas as pd
import os

# import pyarrow

EARLIEST_DATE = "2021.03.01"
LATEST_DATE = "2022.01.30"


def ingestion(override=False, ingestion_360T=True, ingestion_exceed=True):
    # logger = LoggerObj(name=__name__, level="DEBUG")

    intervals = get_month_intervals(earliest_time=EARLIEST_DATE, latest_time=LATEST_DATE)
    tables_360t = ["ClientBestPriceTop3", "ClientDistancetoTOB", "ClientM2M", "FlowQuartileAndLPRelevance",
                   "MappedClientMktShare", "PlatformMktShare", "RelationshipMapping", "flowDB"]
    tables_exceed = ["clientOrder", "orderLegs", "orderAllocation", "cockpitClientMetadata", "cockpitAccountMetadata"]

    kdb = KDB.from_config_file()
    kdb.open()

    try:
        if ingestion_360T:
            # 360T tables
            for table in tables_360t:
                data_ingestion(table=table, intervals=intervals, query_func=kdb.query_table_by_date_range,
                               override=override)
        if ingestion_exceed:
            # exceed tables
            for table in tables_exceed:
                data_ingestion(table=table, intervals=intervals, query_func=kdb.query_exceed_table,
                               override=override)
    except:
        print_exception()
    finally:
        kdb.close()


def data_ingestion(table, intervals, query_func, override=False):
    file_name = "{}.csv".format(table)

    if not override and file_name in os.listdir('data/'):
        print('*** Skipping ingestion for table: {}. File already present ***'.format(table))
        return None

    print('*** Starting ingestion of table: {} ***'.format(table))

    frames = []
    for earliest_time, latest_time in intervals:
        results = query_func(table=table, from_date=earliest_time, to_date=latest_time)
        if type(results) != pd.core.frame.DataFrame and results is None:
            print('\n*** No results found. Aborted ***')
            return None

        print('Found {} results'.format(len(results)))
        frames.append(results) if len(results) > 0 else None

    df = pd.concat(frames)

    try:
        df = df.drop_duplicates()
    except TypeError:
        print('Warning: Cannot drop duplicates')
    except:
        pass
    finally:
        df = df.reset_index(drop=True)

    df.to_csv("data/{}".format(file_name))

    print('*** Data saved to CSV ***'.format(table))


if __name__ == '__main__':
    ingestion(ingestion_360T=False)
