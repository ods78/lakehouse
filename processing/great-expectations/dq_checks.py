"""Great Expectations DQ gates for LakeHouse silver tables.

Reads the silver table via Trino into pandas and validates a suite with the
GX 1.x fluent API (ephemeral context). Falls back to plain pandas checks when
the gx package is unavailable so the pipeline keeps functioning.

Usage: python dq_checks.py silver_customers   (exit 0 = pass, 1 = fail)
"""
import os
import sys

TABLES = {
    "silver_customers": "lakehouse.silver.customers",
    "bronze_customers": "lakehouse.bronze.customers",
}


def fetch_df(table_fqn):
    import pandas as pd
    from trino.dbapi import connect

    conn = connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="lakehouse-dq",
        catalog="lakehouse",
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_fqn}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def plain_checks(df, table):
    problems = []
    if df.empty:
        problems.append("table is empty")
    for col in ("id", "name", "email"):
        if col in df.columns and df[col].isnull().any():
            problems.append(f"column '{col}' contains nulls")
    if "email" in df.columns:
        bad = df["email"].astype(str).str.contains("@").count() != len(df)
        if not df["email"].astype(str).str.contains("@").all():
            problems.append("column 'email' contains malformed values")
    if problems:
        print("DQ PLAIN FAIL:", "; ".join(problems))
        return 1
    print(f"DQ PLAIN PASS: {len(df)} rows OK")
    return 0


def gx_checks(df, table):
    import great_expectations as gx
    import great_expectations.expectations as gxe

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("lakehouse_dq")
    asset = source.add_dataframe_asset(table)
    batch_def = asset.add_batch_definition_whole_dataframe("full")

    suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="name"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="email"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="email", regex="[^@]+@[^@]+"))

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    result = batch.validate(suite)
    if not result.success:
        for res in result.results:
            if not res.success:
                print(f"DQ FAIL: {res.expectation_config.type} -> {res.result}")
        return 1
    print(f"DQ GX PASS: {len(df)} rows, {len(result.results)} expectations met")
    return 0


def main():
    key = sys.argv[1] if len(sys.argv) > 1 else "silver_customers"
    table = TABLES[key]
    df = fetch_df(table)
    try:
        return gx_checks(df, key)
    except ImportError as exc:
        print(f"great-expectations unavailable ({exc}) — falling back to plain checks")
        return plain_checks(df, key)


if __name__ == "__main__":
    sys.exit(main())
