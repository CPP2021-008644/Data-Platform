import os
import datetime as dt
import pandas as pd
import asyncio
import configparser

import src.tslibv2 as tslib
from src.saverloader import fetch_asyncpg

# Negative points for asyncpg
try:
    config = configparser.ConfigParser()
    config.read(os.getenv("PGSERVICEFILE"))
    asyncpgparams = {
        k.replace("dbname", "database"): v
        for k, v in config["ptplt"].items()
        if k != "options"
    }
except Exception:
    asyncpgparams = None

dbconnections = {
    "postgres": {
        "dtpltdata": {
            "psycopg.connect": {"service": "ptplt"},
            "sqlengine": "postgresql:///?service=main",
            "asyncpg": asyncpgparams,
        },
    }
}

dbcon = dbconnections["postgres"]["dtpltdata"]


class DataPlatformDB:
    """
    This class centralizes common read/write operations against the configured
    PostgreSQL-backed data platform, delegating to :mod:`src.tslibv2` helpers
    for actual I/O.

    Attributes
    ----------
    tlib : tslib.TSLib
        Helper used to perform database operations, initialized with
        the connection settings in the global ``dbcon`` mapping.

    Notes
    -----
    Requires a valid ``PGSERVICEFILE`` environment variable and a service
    named ``ptplt`` in that file (or compatible settings in ``dbcon``).
    """

    def __init__(self):
        self.tlib = tslib.TSLib(dbcon["psycopg.connect"])

    def updater(self, instrument, tabtype, df):
        """
        Update table values for a given instrument and type with the provided rows.

        The target table name is derived as ``f"{instrument}_{tabtype}"``,
        lowercased, with ``'-'`` in the instrument replaced by ``'__'``.

        Parameters
        ----------
        instrument : str
            Instrument identifier. Any hyphens are replaced by double
            underscores to build the table name.
        tabtype : str
            Table type. Each table type is associated with a list of column names and types,
            as defined in TableCols from tslibv2.py. 
            ``tabtype``, combined with ``instrument``, forms the table name.
        df : pandas.DataFrame
            Rows to write to the table.

        Returns
        -------
        None

        Exceptions
        ----------
        Any exception raised by the underlying write is caught and printed.
        """
        try:
            instrument = instrument.replace("-", "__")
            tablename = f"{instrument}_{tabtype}".lower()

            self.tlib.update_table(df=df, tablename=tablename)
        except Exception as e:
            print(
                "Found {0} exception while trying to update data for {1}_{2}. \nException Message:\n{3}".format(
                    type(e), instrument, tabtype, e
                )
            )

    def upserter(self, instrument, tabtype, df, **kwargs):
        """
        Insert or update table rows for a given instrument and type with the provided rows. 
        If the table is not created, it creates the table.

        Parameters
        ----------
        instrument : str
            Instrument identifier. Any hyphens are replaced by double
            underscores to build the table name.
        tabtype : str
            Table type. Each table type is associated with a list of column names and types,
            as defined in TableCols from tslibv2.py. 
            ``tabtype``, combined with ``instrument``, forms the table name.
        df : pandas.DataFrame
            Input data to be upserted.

        Returns
        -------
        dict
            The normalized payload returned by :func:`tslib.kafka4db` with
            the ``"my_df"`` key removed. TODO revisar que devuelve

        Notes
        -----
        Prints a summary message on success.

        Exceptions
        ----------
        Any exception raised by normalization or upsert is caught and printed.
        """
        try:
            tablename = "{0}_{1}".format(instrument.replace("-", "__"), tabtype)
            cleandf = tslib.kafka4db(tablename, tabtype, df)

            self.tlib.upsert_remotelight(**{**cleandf, **kwargs})
            
            print(
                f"{len(df)} Rows data for {instrument}_{tabtype} upserted successfully."
            )
        except Exception as e:
            print(
                f"Found {type(e)} exception while trying to add data for {instrument}_{tabtype}. \nException Message:\n{e}"
            )

        cleandf.pop("my_df", None)
        return cleandf

    class loader_input:
        """
        Container for loader request parameters.
        """
        def __init__(
            self,
            table,
            start_date=None,
            end_date=None,
        ):
            self.table = table
            self.start_date = start_date
            self.end_date = end_date

    def loader(self, data):
        """
        Load one or more tables (optionally date-filtered) into DataFrames.

        Builds and executes a ``SELECT *`` for each requested table, applying
        a date range on the ``dtime`` column when provided, and returns a
        mapping of table name to :class:`pandas.DataFrame`.

        Parameters
        ----------
        data : list of DataPlatformDB.loader_input
            Requests to execute. For each item, ``table`` is required and
            ``start_date``/``end_date`` are optional.

        Returns
        -------
        dict[str, pandas.DataFrame]
            Dictionary mapping each table name to its loaded DataFrame,
            ordered by ``dtime`` ascending.
        """

        sql = """\
                SELECT * FROM {0} {1} {2}\
                ORDER BY {3} ASC
                """
        # 0:table name
        # 1: instrumentos
        # 2: WHERE dtime
        # 3: order by

        reqdf = pd.DataFrame(columns=["table", "start_date", "end_date"])
        # if we have less than 3 inputs in the tuple add a None
        for loader_input in data:
            row = [loader_input.table, loader_input.start_date, loader_input.end_date]
            reqdf.loc[len(reqdf)] = row

        reqs = []
        for index, row in reqdf.iterrows():
            prefix = "WHERE"
            instruments_filter = ""

            if row["start_date"] is None:
                date_filter = ""
            else:
                if row["end_date"] is None:
                    row["end_date"] = row["start_date"]
                date_filter = f"""{prefix} dtime >= '{row["start_date"]}' AND dtime < '{row["end_date"] + dt.timedelta(days=1)}' """
                prefix = "AND"

            order_filter = "dtime"

            reqs.append(
                [
                    sql.format(
                        row["table"],
                        instruments_filter,
                        date_filter,
                        order_filter,
                    ),
                    row["table"],
                    False,
                ]
            )

        dfdict = {}
        for req in reqs:
            dfdict.update(asyncio.run(fetch_asyncpg(dbcon["asyncpg"], *req)))
        return dfdict
