import asyncpg
import pandas as pd


def split_df(
    df: pd.DataFrame, split_key: str, tabtype: str, instruments: list = None
) -> dict:
    """Split dataframe into dictionary of dataframes based on unique values of split_key column"""
    dictkeys = {i.lower(): f"{i}_{tabtype}" for i in instruments}

    dfdict = {dictkeys[k]: v for k, v in df.groupby(df[split_key].str.lower())}
    for tablename in list(dictkeys.values()):
        if tablename not in dfdict:
            dfdict[tablename] = pd.DataFrame(
                data=None, columns=df.columns, index=df.index
            )
    return dfdict


async def fetch_asyncpg(
    connection_params: dict,
    query: str,
    tablename: str,
    split: bool,
    instruments: list = None,
) -> dict:
    conn = await asyncpg.connect(**connection_params)
    stmt = await conn.prepare(query)
    cols = [c.name for c in stmt.get_attributes()]
    records = await conn.fetch(query)

    await conn.close()

    df = pd.DataFrame(records, columns=cols)

    if "dtime" in cols:
        df = df.set_index("dtime", inplace=False)

    if split:
        return split_df(
            df, "instrument", tabtype=tablename.split("_")[-1], instruments=instruments
        )
    else:
        return {tablename: df}
