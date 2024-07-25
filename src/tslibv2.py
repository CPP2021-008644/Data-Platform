"""
TSLib is a class to interact with PostgreSQL Database with the TimescaleDB extension.

Mainly this class is used to insert data into the database. It has a multithread mode
with a queue where data can be stored while database insert them. Or you can insert data
sequentially using insert_remote method directly.

Also we created a Timescale class to store auxiliary functions related to the TimescaleDB
functionalities like decompress and compress hypertable chunks.

To create a new table from scratch in the database, the following steps should be followed:
Notes:
    - The table must have a primary key named `dtime`.
Steps:
    1. Add the table in the `TableCols` dictionary.
    2. Add data in the `timescale_config` dictionary.
    3. If `dtime` is of type `date`, add the table name in the `tabtype_date` list.
    4. Assign primary keys using the `get_primary_key` function.
    5. Assign the type of each column in the `createdbifneeded` function.
       - IMPORTANT: Each column name in the entire database 
         can only have one type.
       - NOTE: If it is a numeric type, you do not need to add it.
    6. Add the compression type in `enable_compression` (OPTIONAL).

"""

from abc import abstractmethod
import datetime as dt
from multiprocessing import Queue as queue
import pandas as pd
import psycopg
import threading
from typing import Union
import gc


# Dictionary tabtype:[columns]
TableCols = {
    "prob": [
        "dtime",
        "contract_id",
        "contract_name",
        "price",
        "size",
    ],
}
tabtype_date = []  # list with tabtypes whose index dtime type is date

timescale_config = {"prob": {"compression_policy": "1d", "chunk_interval": "7d"}}


def get_dtime_type(tabtype):
    if tabtype in tabtype_date:
        dtime_type = "DATE"
    else:
        dtime_type = "timestamptz"

    return dtime_type


def get_primary_key(tabtype: str) -> str:
    """Returns the primary keys of the tables from a given tabtype"""
    if tabtype == "prob":
        primary_key = "(dtime, contract_id)"
    else:
        primary_key = "(dtime)"

    return primary_key


def sadbqueuer(instrument: str, tabtype: str, ticks: dict, dbqueue: queue):
    """Will require TableCols to be defined and pandas imported as pd"""

    tablename = f"{instrument}_{tabtype}".lower()
    tabcols = TableCols[tabtype]

    mydf = pd.DataFrame.from_dict(ticks, orient="index")
    mydf[[a for a in tabcols[1:] if a not in mydf.columns]] = None
    mydf = mydf[tabcols[1:]]
    mydf.index.name = "dtime"

    dbqueue.put(
        (tablename, tabtype, tabcols, mydf.to_csv(sep="\t", header=False), mydf)
    )


def preparedf(mydf, tabcols, primary_keys=[]):
    mydf[[a for a in tabcols if a not in mydf.columns]] = None

    if primary_keys:
        mydf = mydf.drop_duplicates(subset=primary_keys)
    if "dtime" in tabcols:
        mydf = mydf[tabcols].set_index("dtime")
    elif "instrument" in tabcols:
        mydf = mydf[tabcols].set_index("instrument")

    return mydf


def kafka4db(tablename: str, tabtype: str, mydf: pd.DataFrame):
    tablename = tablename.lower()
    tabcols = TableCols[tabtype]

    primary_key = get_primary_key(tabtype)
    primary_keys = [a.strip() for a in primary_key[1:-1].split(",")]

    mydf = preparedf(mydf.copy(), tabcols, primary_keys)

    return {
        "tablename": tablename,
        "tabtype": tabtype,
        "tabcols": tabcols,
        "my_csv": mydf.to_csv(sep="\t", header=False),
        "my_df": mydf,
    }


def dfdbqueuer(tablename: str, tabtype: str, mydf: pd.DataFrame, dbqueue: queue):
    """Will require TableCols to be defined and pandas imported as pd"""

    tablename = tablename.lower()
    tabcols = TableCols[tabtype]

    mydf[[a for a in tabcols if a not in mydf.columns]] = None
    mydf = mydf[tabcols]

    dbqueue.put(
        (
            tablename,
            tabtype,
            tabcols,
            mydf.to_csv(sep="\t", header=False, index=False),
            mydf,
        )
    )


def indextodt(df):
    df.reset_index(inplace=True)
    df["dtime"] = pd.to_datetime(df["dtime"])
    df.set_index("dtime", inplace=True)
    return df


class TSLib(object):
    """
    Handles data ingestion into the TimescaleDB database

    Args:
        connectionkwargs: dictionary of connection parameters
        schema (str), default 'primarydata':
            database schema where data has to be stored.
        handle_duplicates_by (str) 'local' or 'db', default 'local':
            Indicates to handle duplicates beforehand or within the database.
            More details in the functions '_handle_UniqueViolation_from_local' and '_handle_UniqueViolation_from_db'
        multithread (bool), default True:
            Enable multithread mode to initialize the queue.
            If an error occurs in this mode. It continues to the next element in the queue.
    """

    def __init__(
        self,
        connectionkwargs,
        handle_duplicates_by: str = "local",
        multithread: bool = True,
        **kwargs,
    ):
        self.tables = set()

        self.continue_if_error = False
        self.handle_duplicates_by = handle_duplicates_by
        self.multithread = multithread

        ## Connection Part
        self.connkwargs = connectionkwargs

        self.conn = psycopg.connect(**self.connkwargs)

        try:
            ### Create the timescaledb extension if needed.
            self.conn.cursor().execute(
                "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
            )

            ### Apply changes.
            self.conn.commit()
        except:
            self.conn.rollback()

        self.lasqueinf = dt.datetime.now(tz=dt.timezone.utc).replace(
            second=0, microsecond=0
        )
        self.quinfointerval = dt.timedelta(seconds=60)

        self.processqueue = self.multithread

        ## Thread safety part

        if self.multithread:
            self.continue_if_error = True
            self.queue = queue()
            self.processthread = threading.Thread(
                target=self.queueprocessing, args=([]), daemon=True, name="libproc"
            )
            self.processthread.start()

    def __del__(
        self,
    ):
        if self.multithread:
            self.queueprocessing = False
            self.processqueue = False
            self.queue.put(None)
            self.processthread.join()
            self.queue.close()
            self.queue.join_thread()
        self.conn.close()

    def handle_exceptions(self, e: Exception, *args, **kwargs):
        """Handle the exception according the exception class.
        Expected exceptions are:
            - psycopg.errors.UniqueViolation:
                · Raised when: primary key violation occurs during the insertion.
                · Fix: Insert just the new data ignoring the data that violates
                        the primary key.

            - psycopg.errors.FeatureNotSupported:
                · Raise when: trying to insert into a hypertable chunk that is compressed.
                · Fix: decompresss the corresponding chunk first.

        When unknown exceptions occurs, we just raise up to stop the process (single thread)
        or continue to the next data object from que queue (multithread mode).
        """

        self.conn.rollback()
        if isinstance(e, psycopg.errors.UniqueViolation):
            self.__manage_UniqueViolation_exception(*args, **kwargs)
        elif isinstance(e, psycopg.errors.FeatureNotSupported):
            self.__manage_FeatureNotSupported_exception(*args, **kwargs)
        elif isinstance(e, psycopg.errors.OperationalError):
            self.__manage_OperationalError_exception(*args, **kwargs)
        else:
            print("TSLib - Unknown error:", e)
            if not self.continue_if_error:
                self.conn.close()
                raise e

    def _print_inserted_rows(
        self,
        df: pd.DataFrame,
        tablename: str,
        inserted_rows: int = None,
        insertion: str = "full",
    ) -> None:
        """
        Message printed when data has been inserted correctly.
        It shows the numbers of rows inserted, in which table and the date range.
        Also displays a warning when when all the data could not be inserted
        when UniqueViolation occurs.

        Args:
            df (pd.DataFrame):
                DataFrame with the data inserted
            tablename (str):
                Name of the table where data has been stored.
            inserted_rows (int), optional:
                Number of rows inserted. When None it is assumed that all the
                DataFrame has been stored so it takes its length.
            insertion (str) 'full' or 'partial', default 'full':
                Indicates if the insertion has been 'full' or 'partial'.
        """

        if not inserted_rows:
            inserted_rows = len(df)

        msg = f"INSERTED - {inserted_rows}\t- {tablename}\t"

        if inserted_rows > 0:
            msg += f"- {df.index.min().date()} : {df.index.max().date()}"

        if insertion == "partial":
            if inserted_rows > 0:
                msg += " (WARNING - UniqueViolation: Some data not inserted)"
            else:
                msg += " (WARNING - UniqueViolation: No data inserted)"

        print(msg)

    def _copy_to_db(
        self,
        df,
        tablename,
        my_csv=None,
        tabcols=None,
        print=False,
        insertion="full",
        *args,
        **kwargs,
    ) -> None:
        """
        Insert the given DataFrame into the database using PostgreSQL COPY command.
        Prints a message when is done.

        Args:
            df (pd.DataFrame):
                DataFrame we want to store
            tablename (str):
                SQL Table name where we want to insert it.
            my_csv (str), optional:
                CSV Path (To use instead of the Dataframe), if None use Pandas df.to_csv()
            tabcols (str), optional:
                Columns of corresponding SQL table. if None it is infer from the table name.
            print (bool), default True:
                Indicates if prints a message after insert the data.
            insertion (str) 'full' or 'partial', default 'full':
                Indicates if the insertion has been 'full' or 'partial'.
        """

        if not tabcols:
            tabtype = tablename.split("_")[-1]
            tabcols = TableCols[tabtype]

        if not my_csv:
            my_csv = df.to_csv(sep="\t", header=False)

        cols_str = ", ".join('"{}"'.format(k) for k in tabcols)

        try:
            with self.conn.cursor() as cur:
                with cur.copy(
                    f"""COPY {tablename}({cols_str}) FROM STDIN WITH NULL AS '';"""
                ) as copy:
                    copy.write(my_csv)

            self.conn.commit()

            if print:
                self._print_inserted_rows(df, tablename, insertion=insertion)

        except Exception as e:
            self.handle_exceptions(e, df, tablename)

    def _copy_to_dblight(
        self, tablename, my_csv, tabcols, my_df, *args, **kwargs
    ) -> None:
        """
        Insert the given DataFrame into the database using PostgreSQL COPY command.
        Prints a message when is done.

        Args:
            tablename (str):
                SQL Table name where we want to insert it.
            my_csv (str):
                CSV to insert (To use instead of the Dataframe), if None use Pandas df.to_csv()
            tabcols (str):
                Columns of corresponding SQL table. if None it is infer from the table name.
        """

        cols_str = ", ".join('"{}"'.format(k) for k in tabcols)

        try:
            with self.conn.cursor() as cur:
                with cur.copy(
                    f"""COPY {tablename}({cols_str}) FROM STDIN WITH NULL AS '';"""
                ) as copy:
                    copy.write(my_csv)

            self.conn.commit()

        except Exception as e:
            try:
                self.handle_exceptions(
                    e, my_df, tablename=tablename, my_csv=my_csv, tabcols=tabcols
                )
            except Exception as exxx:
                print(exxx)

    def _upsert_dblight(
        self, tablename, my_csv, tabcols, my_df, *args, **kwargs
    ) -> None:
        """
        Insert the given DataFrame into the database using PostgreSQL COPY command.
        Prints a message when is done.

        Args:
            tablename (str):
                SQL Table name where we want to insert it.
            my_csv (str):
                CSV to insert (To use instead of the Dataframe), if None use Pandas df.to_csv()
            tabcols (str):
                Columns of corresponding SQL table. if None it is infer from the table name.
        """

        cols_str = ", ".join('"{}"'.format(k) for k in tabcols)

        # Get the primary key according to tabtype
        tabtype = tablename.split("_")[-1]
        primary_key = get_primary_key(tabtype)
        primary_keys = [a.strip() for a in primary_key[1:-1].split(",")]

        nonkeycols = [a for a in tabcols if a not in primary_keys]

        # SQL Queries
        tmp_table = tablename + "_tmp"
        create_tmp_table = f"""
            CREATE TEMP TABLE {tmp_table} 
            (LIKE {tablename} INCLUDING DEFAULTS)
            ON COMMIT DROP;
        """

        updaterow = ",\n".join(
            [
                f"{col} = COALESCE(excluded.{col}, {tablename}.{col})"
                for col in nonkeycols
            ]
        )
        wherestr = """WHERE {0};""".format(
            " AND ".join(
                [f"{tablename}.{keycol} = excluded.{keycol}" for keycol in primary_keys]
            )
        )

        if not nonkeycols:  # case only PK are stored
            onconflict = f"""
                INSERT INTO {tablename}({cols_str})
                SELECT {cols_str}
                FROM {tmp_table}
                ON CONFLICT {primary_key}
                DO NOTHING
                """
        else:
            onconflict = f"""
                INSERT INTO {tablename}({cols_str})
                SELECT {cols_str}
                FROM {tmp_table}
                ON CONFLICT {primary_key}
                DO UPDATE SET
                {updaterow}
                {wherestr}
                """

        try:
            with self.conn.cursor() as cur:
                cur.execute(create_tmp_table)

                with cur.copy(
                    f"""COPY {tmp_table}({cols_str}) FROM STDIN WITH NULL AS '';"""
                ) as copy:
                    copy.write(my_csv)

                cur.execute(onconflict)

            self.conn.commit()

        except Exception as e:
            try:
                self.handle_exceptions(
                    e, my_df, tablename=tablename, my_csv=my_csv, tabcols=tabcols
                )
            except Exception as exxx:
                print(exxx)

    def _update_dblight(
        self, tablename, my_csv, tabcols, my_df, *args, **kwargs
    ) -> None:
        """
        Insert the given DataFrame into the database using PostgreSQL COPY command.
        Prints a message when is done.

        Args:
            tablename (str):
                SQL Table name where we want to insert it.
            my_csv (str):
                CSV to insert (To use instead of the Dataframe), if None use Pandas df.to_csv()
            tabcols (str):
                Columns of corresponding SQL table. if None it is infer from the table name.
        """

        cols_str = ", ".join('"{}"'.format(k) for k in tabcols)

        # Get the primary key according to tabtype
        tabtype = tablename.split("_")[-1]
        primary_key = get_primary_key(tabtype)
        primary_keys = [a.strip() for a in primary_key[1:-1].split(",")]

        nonkeycols = [a for a in tabcols if a not in primary_keys]

        # SQL Queries
        tmp_table = tablename + "_tmp"
        create_tmp_table = f"""
            CREATE TEMP TABLE {tmp_table} 
            (LIKE {tablename} INCLUDING DEFAULTS)
            ON COMMIT DROP;
        """

        updaterow = ",\n".join([f"{col} = {tmp_table}.{col}" for col in nonkeycols])
        wherestr = """WHERE {0};""".format(
            " AND ".join(
                [
                    f"{tablename}.{keycol} = {tmp_table}.{keycol}"
                    for keycol in primary_keys
                ]
            )
        )
        updatetable = f"""
            UPDATE {tablename}
            SET
            {updaterow}
            FROM {tmp_table}
            {wherestr}
            """

        try:
            with self.conn.cursor() as cur:
                cur.execute(create_tmp_table)

                with cur.copy(
                    f"""COPY {tmp_table}({cols_str}) FROM STDIN WITH NULL AS '';"""
                ) as copy:
                    copy.write(my_csv)

                cur.execute(updatetable)

            self.conn.commit()

        except Exception as e:
            try:
                self.handle_exceptions(
                    e, my_df, tablename=tablename, my_csv=my_csv, tabcols=tabcols
                )
            except Exception as exxx:
                print(exxx)

    def update_table(
        self,
        df: pd.DataFrame,
        tablename: str,
    ):
        """
        Update a table in the database with the data in the dataframe.
        The dataframe may have less columns as the table and the may have not all the
        primary keys.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with the data to update the table
        table : str
            Name of the table to update
        conn : any
            Connection to the database

        """

        mydf = df.where(pd.notnull(df), None)
        tabtype = tablename.split("_")[-1]
        tabcols = TableCols[tabtype]
        # Create if needed
        self.createdbifneeded(tablename, tabtype)

        # we don't care about the columns not int tabcols
        goodcols = [col for col in mydf.columns if col in tabcols]

        mydf = mydf[goodcols]

        self._update_dblight(
            tablename, mydf.to_csv(sep="\t", header=False, index=False), goodcols, mydf
        )


    def _prepare_dataframe(self, df: pd.DataFrame, tabcols: str) -> pd.DataFrame:
        """Prepare the dataframe before inserted:
        1. Check it is not empty
        2. Check column names
        3. Add missing columns as None"""

        # checking if df is not empty and has a dtime column/index
        try:
            assert not df.empty, "TSLib - ERROR: DataFrame received is empty."
            assert "dtime" in df.columns or df.index.name == "dtime", (
                "TSLib - ERROR: DataFrame must have dtime column or index"
            )
        except AssertionError as e:
            print(str(e))
            raise e

        # make dtime index if not already
        if df.index.name != "dtime":
            df.set_index("dtime", inplace=True)

        df.index = pd.to_datetime(df.index)

        # Check columns and add missing ones as None
        df = df[tabcols[1:]]
        df[[col for col in tabcols[1:] if col not in df.columns]] = None

        return df

    def insert_remote(
        self,
        df: pd.DataFrame,
        tablename: str,
        my_csv: str = None,
        tabtype: str = None,
        tabcols: str = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Inserts given DataFrame into the database.

        Steps:
            1. Prepare the DataFrame
            2. Create the table if not exists yet
            3. Insert data

        Args:
                df (pd.DataFrame): DataFrame we want to store
                tablename (str): SQL Table name where we want to insert it.
                my_csv (str), optional: CSV (To use instead of the Dataframe), if None use Pandas df.to_csv()
                tabtype (str), optional: table type. if None it is infer from the table name.
                tabcols (str), optional: Columns of corresponding SQL table. if None it is infer from the table type.
        """

        # infer tabtype from tablename if not passed.
        if not tabtype:
            tabtype = tablename.split("_")[-1]

        if not tabcols:
            tabcols = TableCols[tabtype]

        # prepare df if needed, check columns and set dtime as index
        if list(df.columns[:1]) != tabcols or df.index.name != "dtime":
            df = self._prepare_dataframe(df, tabcols)

        ### Create table if not exist
        self.createdbifneeded(tablename, tabtype)

        ### Copy to the db.
        self._copy_to_db(df, tablename, my_csv, tabcols)

    def insert_remotelight(
        self,
        tablename: str,
        my_csv: str,
        tabtype: str,
        tabcols: str,
        my_df: pd.DataFrame,
        *args,
        **kwargs,
    ) -> None:
        """
        Inserts given DataFrame into the database.

        Steps:
            1. Prepare the DataFrame
            2. Create the table if not exists yet
            3. Insert data

        Args:
                tablename (str): SQL Table name where we want to insert it.
                my_csv (str), : CSV string to insert (created using Pandas df.to_csv())
                tabtype (str), optional: table type. if None it is infer from the table name.
                tabcols (str), optional: Columns of corresponding SQL table. if None it is infer from the table type.
        """

        ### Create table if not exist
        self.createdbifneeded(tablename, tabtype)

        ### Copy to the db.
        self._copy_to_dblight(tablename, my_csv, tabcols, my_df)

    def upsert_remotelight(
        self,
        tablename: str,
        my_csv: str,
        tabtype: str,
        tabcols: str,
        my_df: pd.DataFrame,
        *args,
        **kwargs,
    ) -> None:
        """
        Inserts given DataFrame into the database.

        Steps:
            1. Prepare the DataFrame
            2. Create the table if not exists yet
            3. Insert data

        Args:
                tablename (str): SQL Table name where we want to insert it.
                my_csv (str), : CSV string to insert (created using Pandas df.to_csv())
                tabtype (str), optional: table type. if None it is infer from the table name.
                tabcols (str), optional: Columns of corresponding SQL table. if None it is infer from the table type.
        """

        istemp = kwargs.get("istemp", False)

        tempparams = {}
        if istemp:
            tempparams = {"retention": True, "compress": False}

        ### Create table if not exist
        self.createdbifneeded(tablename, tabtype, **tempparams)

        ### Copy to the db.
        self._upsert_dblight(tablename, my_csv, tabcols, my_df)

    def __manage_OperationalError_exception(self, *args, **kwargs):
        self.conn = psycopg.connect(**self.connkwargs)  # Regenerate connection
        tablename = kwargs.get("tablename", "")
        my_csv = kwargs.get("my_csv", "")
        tabcols = kwargs.get("tabcols", "")
        my_df = kwargs.get("my_df", pd.DataFrame(columns=["dtime"]).set_index("dtime"))

        if tablename and my_csv and tabcols:
            self._copy_to_dblight(tablename, my_csv, tabcols, my_df)

    def __manage_FeatureNotSupported_exception(
        self,
        df: pd.DataFrame,
        tablename: str,
        csv_path: str = None,
        tabcols: str = None,
        *args,
        **kwargs,
    ):
        """Handles psycopg.errors.FeatureNotSupported when we
        are trying to do backfilling"""

        self.backfilling(df, tablename)

    def __manage_FeatureNotSupported_exception(
        self,
        df: pd.DataFrame,
        tablename: str,
        csv_path: str = None,
        tabcols: str = None,
        *args,
        **kwargs,
    ):
        """Handles psycopg.errors.FeatureNotSupported when we
        are trying to do backfilling"""

        self.backfilling(df, tablename)

    def __manage_UniqueViolation_exception(
        self,
        df: pd.DataFrame,
        tablename: str,
        csv_path: str = None,
        tabcols: str = None,
        *args,
        **kwargs,
    ):
        """Handles psycopg.errors.UniqueViolation according to attribute
        handle_duplicates_by
        """
        if self.handle_duplicates_by == "local":
            self._handle_UniqueViolation_from_local(df, tablename)
        elif self.handle_duplicates_by == "db":
            self._handle_UniqueViolation_from_db(df, tablename)
        else:
            raise ValueError(
                f"mode '{self.handle_duplicates_by}' not valid. Must be 'local' or 'db'."
            )

    def _handle_UniqueViolation_from_local(
        self,
        df: pd.DataFrame,
        tablename: str,
        csv_path: str = None,
        tabcols: str = None,
        *args,
        **kwargs,
    ):
        """Remove duplicated rows from the given dataframe before insert it into
        the database again."""

        sortedidx = df.sort_index().index
        first = pd.to_datetime(sortedidx[0]).to_pydatetime()
        last = pd.to_datetime(sortedidx[-1]).to_pydatetime()

        tabtype = tablename.split("_")[-1]
        tabcols = TableCols[tabtype]
        thekey = get_primary_key(tabtype)

        if "exch_trade_id" in thekey:
            col_to_select = "exch_trade_id"
            keys = df["exch_trade_id"].astype(str)
        elif "cdoferta" in thekey:
            col_to_select = "cdoferta"
            keys = df["cdoferta"].astype(str)
        elif "cdtrans" in thekey:
            col_to_select = "cdtrans"
            keys = df["cdtrans"].astype(str)
        elif "quote_id" in thekey:
            col_to_select = "quote_id"
            keys = df["quote_id"].astype(str)
        elif "toll_path" in thekey:
            col_to_select = "toll_path"
            keys = df["toll_path"].astype(str)
        elif "product" in thekey:
            col_to_select = "product"
            keys = df["product"].astype(str)
        elif "instrument" in thekey:
            col_to_select = "instrument"
            keys = df["instrument"].astype(str)
        else:
            col_to_select = "dtime"
            keys = df.index

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {col_to_select} 
                FROM {tablename} WHERE "dtime" BETWEEN TIMESTAMPTZ '{first}' AND TIMESTAMPTZ '{last}'; 
            """
            )
            givenrows = [str(key[0]) for key in cur.fetchall()]

        mask = ~keys.isin(givenrows)
        df = df[mask]

        if not df.empty:
            self._copy_to_db(
                df,
                tablename,
                df.to_csv(sep="\t", header=False),
                tabcols,
                insertion="partial",
            )
        else:
            self._print_inserted_rows(df, tablename, insertion="partial")

        del givenrows

    def _handle_UniqueViolation_from_db(
        self,
        df: pd.DataFrame,
        tablename: str,
        csv_path: str = None,
        tabcols: str = None,
        *args,
        **kwargs,
    ):
        """Create temporal table with the data to insert and
        then insert from it ignoring duplicates"""

        # Get the primary key according to tabtype
        tabtype = tablename.split("_")[-1]
        primary_key = get_primary_key(tabtype)

        # SQL Queries
        tmp_table = tablename + "_tmp"
        create_tmp_table = f"""
            CREATE TEMP TABLE {tmp_table} ON COMMIT DROP AS
                SELECT * FROM {tablename}
            WITH NO DATA;
        """
        insert_rows_from_tmp_table = f"""
            INSERT INTO {tablename} SELECT * FROM {tmp_table} 
            ON CONFLICT {primary_key} DO NOTHING  
            RETURNING dtime
        """

        with self.conn.cursor() as cur:
            # Create the temporal table
            cur.execute(create_tmp_table)

            # Populate the tmp. table with the data we want to insert.
            self._copy_to_db(
                df, df.to_csv(sep="\t", header=False), tmp_table, tabcols, print=False
            )

            # Insert from the tmp. table in its final table.
            cur.execute(insert_rows_from_tmp_table)
            inserted_rows = len(cur.fetchall())
            self._print_inserted_rows(df, tablename, inserted_rows, insertion="partial")

        self.conn.commit()

    def backfilling(self, df: pd.DataFrame, tablename: str):
        """Perform backfilling insertion into the database using TimescaleDB
        functions to locate the corresponding chunk and decompress it
        before insert the data again."""

        # 1. Stop compression job before decompress chunks
        job_id = Timescale.get_compression_job_id(self.conn, tablename)
        Timescale.pause_compression_job(self.conn, job_id)

        # 2. Decompress chunks where data is going to be stored
        start = df.index.min()
        end = df.index.max()

        Timescale.decompress_chunks(self.conn, tablename, start, end)

        # 3. Insert data
        self._copy_to_db(df, tablename)

        # optional: compress chunks again (slower)
        # self.compress_chunks(tablename, start, end)

        # 4. Re-enable compression job
        Timescale.enable_compression_job(self.conn, job_id)

        self.conn.commit()

    def queueprocessing(self):
        """Function to processing TSlib task queue. TSLib will
        continue running until stop() function is called and
        queue is empty"""

        while not (not self.processqueue and self.queue.empty()):
            try:
                item = self.queue.get(block=True)
                if item and item[1]:
                    (tablename, tabtype, tabcols, mycsv, mydf) = item
                else:
                    continue

                self.insert_remote(mydf, tablename, mycsv, tabcols)

                if (
                    dt.datetime.now(tz=dt.timezone.utc) - self.lasqueinf
                    >= self.quinfointerval
                ):
                    print("TSLib queue size {0}.".format(self.queue.qsize()))
                    self.lasqueinf = dt.datetime.now(tz=dt.timezone.utc).replace(
                        second=0, microsecond=0
                    )

                    gc.collect(generation=2)

                del item, tablename, tabtype, tabcols, mydf, mycsv

            except Exception as exp:
                print("Error en tslib")
                print(exp)
                print(exp.with_traceback())

    def kakfaprocessing(self):
        """Function to process ticks received from a kafka queue.
        The queue processing and multithreading is done externally.

        """

    def createdbifneeded(self, tablename, *args, **kwargs):
        """Create table for a given product and apply the TimescaleDB settings
        according its tabtype.

        Args:
            tablename (str): Name of the table we want to create.
        kwargs:
            compress (bool), default True: If enables TimescaleDB compression
            retention (bool), default False: If enables TimescaleDB data retention
        Returns:
            bool: True if table has been created properly False otherwise
        """

        compress = kwargs.get("compress", True)
        retention = kwargs.get("retention", False)

        if tablename in self.tables:
            return True
        else:
            tabtype = tablename.split("_")[-1]
            if tabtype not in TableCols:
                print(TableCols.keys())
                print(
                    f"DB type {tabtype} not recognized.\nConnection Details:\n{self.conn.info.get_parameters()}"
                )
                return False

            # Postgress type when not decimal
            dtime_type = get_dtime_type(tabtype)
            ctstringbase = """
                    CREATE TABLE IF NOT EXISTS {0} (
                       {1},
                       PRIMARY KEY{2}
                    );
            """
            pstype = {
                "dtime": dtime_type,
                "contract_id": "text",
                "contract_name": "text",
            }

            cur = self.conn.cursor()

            # Checking if table existe
            answer = cur.execute(
                """SELECT EXISTS ( 
                                SELECT FROM timescaledb_information.hypertables 
                                WHERE hypertable_name = (%s));
                    """,
                (tablename,),
            ).fetchone()

            if answer:
                answer = answer[0]

            if not answer:  # If table doesn't exist, create it
                # Preparing the query to create the table
                tablecols = ", ".join(
                    [
                        "{0} {1}".format(a, pstype[a] if a in pstype else "decimal")
                        for a in TableCols[tabtype]
                    ]
                )
                primary_key = get_primary_key(tabtype)
                ctstring = ctstringbase.format(tablename, tablecols, primary_key)

                htstr = "SELECT create_hypertable((%s),'dtime');"
                try:
                    # 1. Create table
                    print(f'TABLE NOT FOUND -> Creating table "{tablename}"...')
                    cur.execute(ctstring)

                    # 2. Making it an hypertable
                    print("\t- Making it an hypertable.")
                    cur.execute(htstr, (tablename,))

                    # 3. Enable compression if enabled
                    if compress:
                        Timescale.enable_compression(cur, tablename, tabtype)

                    # 4. Setting chunk time interval
                    print("\t- Setting chunk interval.")
                    cur.execute(
                        f"SELECT set_chunk_time_interval('{tablename}', INTERVAL '{timescale_config[tabtype]['chunk_interval']}');"
                    )

                    # 5. Enable retention policy if enabled
                    if retention:
                        Timescale.enable_retention(cur, tablename, tabtype)

                    # 6. Apply changes
                    self.conn.commit()

                except Exception as e:
                    self.conn.rollback()
                    print(
                        f"Unable to create table {tablename} of type {tabtype}.\nConnection Details:\n{self.conn.info.get_parameters()}"
                    )
                    raise e

            self.tables.add(tablename)

            return True

    # I implement this method to make it more interface like, since actually it will pass everything to the queue
    def db_writer(self, instrument: str, ticks: dict, tabtype: str):
        """Put given instrument data in the queue
        Args:
            instrument (str): instrument name
            ticks (dict): data in dict shape, keys are the SQL table columns
            tabtype (str): data type
        """

        if self.processqueue:
            # to be protected agains race conditions, the library is going to implement a queue processor on it's own.
            self.queue.put((instrument, ticks, tabtype))
        else:
            # This should not happen, since the processqueue should only become false after everything has been added to the queue.
            print(
                f"We have stopped processing the queue last {len(ticks)} {tabtype} entries for {instrument} will not be added to the database."
            )

    def stop(self):
        """Stop the processing loop. It waits until queue is empty"""

        if self.multithread:
            self.queueprocessing = False
            self.processqueue = False
            self.queue.put(None)
            self.processthread.join()
            self.queue.close()
            self.queue.join_thread()
        self.conn.close()


class Timescale:
    """Class to store multiple auxiliary functions related to TimescaleDB"""

    @abstractmethod
    def enable_compression(cur: any, tablename: str, tabtype: str):
        """Enable compression of a hypertable"""

        print("\t- Enabling compression.")
        cur.execute(
            f"ALTER TABLE {tablename} SET (timescaledb.compress, timescaledb.compress_orderby = 'dtime ASC');"
        )
        print("\t- Setting compression policy.")
        cur.execute(
            f"SELECT add_compression_policy('{tablename}', INTERVAL '{timescale_config[tabtype]['compression_policy']}');"
        )

    @abstractmethod
    def enable_retention(cur: any, tablename: str, tabtype: str):
        """Enable data retention policy of a hypertable"""
        tomorrow = (dt.date.today() + dt.timedelta(days=1)).isoformat()
        interval = timescale_config[tabtype]["compression_policy"]
        cur.execute(
            f"SELECT add_retention_policy('{tablename}', INTERVAL '{interval}', initial_start => '{tomorrow}' );"
        )

    @abstractmethod
    def get_compression_job_id(conn: any, tablename: str) -> int:
        """Returns the compression job id of an hypertable"""

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT j.job_id
                FROM timescaledb_information.jobs j
                INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
                WHERE j.proc_name = 'policy_compression' 
                    AND s.hypertable_name = '{tablename}';  
                """
            )

            job_id = cur.fetchone()[0]

        return job_id

    @abstractmethod
    def pause_compression_job(conn: any, job_id: Union[str, int]) -> None:
        """Stop timescale compression job"""

        with conn.cursor() as cur:
            query = f"SELECT alter_job({job_id}, scheduled => false)"
            cur.execute(query)

    @abstractmethod
    def enable_compression_job(conn: any, job_id: Union[str, int]) -> None:
        """Enable TimescaleDB compression job"""
        with conn.cursor() as cur:
            cur.execute(f"SELECT alter_job({job_id}, scheduled => true) ")

    @abstractmethod
    def decompress_chunks(conn: any, tablename: str, start: str, end: str) -> None:
        """Decompress hypertable chunks corresponding to the range date
        from start to end parameters."""

        tabtype = tablename.split("_")[-1].lower()

        dtime_type = get_dtime_type(tabtype)

        with conn.cursor() as cur:
            query = f"""
            SELECT decompress_chunk(i, true) from 
				show_chunks('{tablename}', 
				    newer_than => ('{start}'::timestamptz - interval '{timescale_config[tabtype]["chunk_interval"]}')::{dtime_type}, 
				    older_than => ('{end}'::timestamptz + interval '{timescale_config[tabtype]["chunk_interval"]}')::{dtime_type}) i;  
            """
            try:
                cur.execute(query)
            except (
                psycopg.errors.ObjectNotInPrerequisiteState
            ):  # Chunk is already decompressed
                conn.rollback()

    @abstractmethod
    def compress_chunks(conn: any, tablename: str, start: str, end: str) -> None:
        """Decompress hypertable chunks corresponding to the range date
        from start to end parameters."""

        tabtype = tablename.split("_")[-1].lower()
        dtime_type = get_dtime_type(tabtype)

        with conn.cursor() as cur:
            query = f"""
            SELECT compress_chunk(i, true) from 
				show_chunks('{tablename}', 
				    newer_than => ('{start}'::timestamptz - interval '{timescale_config[tabtype]["chunk_interval"]}')::{dtime_type}, 
				    older_than => ('{end}'::timestamptz + interval '{timescale_config[tabtype]["chunk_interval"]}')::{dtime_type}) i;  
            """
            try:
                cur.execute(query)
            except (
                psycopg.errors.ObjectNotInPrerequisiteState
            ):  # Chunk is already compressed
                conn.rollback()
