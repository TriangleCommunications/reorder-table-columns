#!/usr/bin/env python3

import os
import re
import subprocess
from inspect import cleandoc
from typing import List, Tuple, Optional

import click
import psycopg2
import psycopg2.extras

def find(file, paths):
    for path in paths:
        for root, dirs, files in os.walk(path):
            if file in files:
                return os.path.join(root, file)


def get_pg_dump() -> str:
    if os.name == "nt":
        file = "pg_dump.exe"
        paths = os.environ["PATH"].split(";")
        paths.append("C:\\Program Files\\PostgreSQL")
    else:
        file = "pg_dump"
        paths = os.environ["PATH"].split(":")
    exe = find(file, paths)
    return exe


def get_dump_sql(host: str, port: int, database: str, schema: str, table: str, user: str) -> str:
    """Get SQL that would be returned with `pg_dump`"""
    result = subprocess.run(
        [
            get_pg_dump(),
            f"--host={host}",
            f"--port={port}",
            f"--username={user}",
            "--schema-only",
            f"--table={schema}.{table}",
            database
        ],
        capture_output=True,
        check=True,
        text=True,
    )
    return result.stdout


def get_columns(
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    user: str
) -> Tuple[List[str], List[str]]:
    """Get columns for a table"""
    sql_text = get_dump_sql(host, port, database, schema, table, user)
    table_re = re.compile(
        fr"(?P<pre>(?:\n|.)+CREATE TABLE {schema}.{table}\s+\(\n)(?P<rows>(?:\n|.)+?)(?P<post>\);(?:\n|.)+)"
    )
    match = table_re.search(sql_text)
    if match is None:
        raise RuntimeError(f"Could not find table {schema}.{table}")
    return (
        [
            row.strip().strip(",")
            for row in match.group("rows").strip().split("\n")
            if not row.strip().startswith(("CONSTRAINT"))
        ],
        [
            row.strip().strip(",")
            for row in match.group("rows").strip().split("\n")
            if row.strip().split()[0].isupper()
        ]
    )


def get_migration_sql(
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    user:str,
    password: str,
    columns: List[str],
    extras: List[str]
) -> str:
    """Get SQL command to migrate a source table into the target table"""
    sql_text = get_dump_sql(host, port, database, schema, table, user)
    table_re = re.compile(
        fr"(?P<pre>(?:\n|.)+)(?P<table>CREATE TABLE {schema}\.{table}\s+\(\n(?:\n|.)+?\);)(?P<post>(?:\n|.)+)"
    )
    match = table_re.search(sql_text)

    not_nulls = "\n".join(
        [
            cleandoc(
                f"""
            ALTER TABLE {schema}.{table}_migration
            ALTER {nn['column_name']} SET NOT NULL;
        """
            )
            for nn in get_not_null_columns(host, port, database, schema, table, columns, user, password)
        ]
    )
    fk_disable = "\n".join(
        [
            cleandoc(
                f"""
            ALTER TABLE {fk['schema']}.{fk['local_table']}
            DROP CONSTRAINT {fk['constraint']};
        """
            )
            for fk in get_foreign_keys(host, port, database, schema, table, user, password)
        ]
    )
    fk_enable = "\n".join(
        [
            cleandoc(
                f"""
            ALTER TABLE {fk['schema']}.{fk['local_table']}
            ADD CONSTRAINT {fk['constraint']} FOREIGN KEY ({fk['local_column']})
            REFERENCES {fk['schema']}.{fk['foreign_table']} ({fk['foreign_column']});
        """
            )
            for fk in get_foreign_keys(host, port, database, schema, table, user, password)
        ]
    )
    extra_features = "\n".join(
        [
            f"ALTER TABLE {schema}.{table} ADD {row};"
            for row in extras
        ]
    )
    indexes = "\n".join(
        [
            cleandoc(
                ix['index_def']
            )
            for ix in get_indexes(host, port, database, schema, table, columns, user, password)
        ]
    )

    cols = [row.split()[0] for row in columns]
    return cleandoc(
        f"""
{match.group("pre")}
-- Create new table with data from old one
CREATE TABLE {schema}.{table}_migration AS
SELECT {', '.join(cols)}
FROM {schema}.{table};

-- Add NOT NULL constraints to new table
{not_nulls}

/*
 * WARNING: Removing foreign keys from old table
 */
-- Disable foreign keys on the old table
{fk_disable}

/*
 * WARNING: Droppping the old table
 */
-- Drop the old table
DROP TABLE {schema}.{table};

-- Rename new table
ALTER TABLE {schema}.{table}_migration RENAME TO {table};

-- Add extra features (constraints) back
{extra_features}
{match.group("post")}

-- Add foreign keys back
{fk_enable}

-- Add indexes back
{indexes}
        """
    )

def get_foreign_keys(
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    user: str,
    password: str
) -> List[dict]:
    """Get foreign keys referencing a given table"""
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(
                """
                SELECT
                    tc.table_schema AS schema,
                    tc.constraint_name AS constraint,
                    tc.table_name AS local_table,
                    kcu.column_name AS local_column,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
                WHERE constraint_type = 'FOREIGN KEY'
                AND ccu.table_schema = %s
                AND ccu.table_name = %s
            """,
                (schema,table,),
            )
            return [dict(row) for row in curs.fetchall()]

def get_not_null_columns(
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    columns: List[str],
    user: str,
    password: str
) -> List[dict]:
    cols = [row.split()[0] for row in columns]
    """Get foreign keys referencing a given table"""
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(
                f"""
                SELECT
                    c.column_name
                FROM information_schema.columns c
                WHERE c.table_schema = '{schema}'
                AND c.table_name = '{table}'
                AND c.column_name IN ('{"','".join(cols)}')
                AND c.is_nullable = 'NO'
            """,
                (),
            )
            return [dict(row) for row in curs.fetchall()]

def get_indexes(
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    columns: List[str],
    user: str,
    password: str
) -> List[dict]:
    cols = [row.split()[0] for row in columns]
    """Get indeexs for a given table"""
    query = f"""
WITH
      index_column_indexes AS (
        SELECT
              x.indexrelid
            , generate_series(1, x.indnatts) AS attribute_index
        FROM
            pg_catalog.pg_index x
            JOIN pg_catalog.pg_class c
                ON c.oid = x.indrelid
            JOIN pg_catalog.pg_class i
                ON i.oid = x.indexrelid
            JOIN pg_catalog.pg_namespace n
                ON n.oid = c.relnamespace
        WHERE
                (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"]))
            AND (i.relkind = ANY (ARRAY['i'::"char", 'I'::"char"]))
            AND n.nspname = '{schema}'
            AND c.relname = '{table}'
            AND x.indisprimary = FALSE
      )
    , index_column_names AS (
        SELECT
              x.indexrelid
            , array_agg(pg_get_indexdef(i.oid, ici.attribute_index, false)) AS column_names
        FROM
            pg_catalog.pg_index x
            JOIN pg_catalog.pg_class c
                ON c.oid = x.indrelid
            JOIN pg_catalog.pg_class i
                ON i.oid = x.indexrelid
            JOIN pg_catalog.pg_namespace n
                ON n.oid = c.relnamespace
            INNER JOIN index_column_indexes ici
                ON ici.indexrelid = x.indexrelid
        WHERE
                (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"]))
            AND (i.relkind = ANY (ARRAY['i'::"char", 'I'::"char"]))
            AND n.nspname = '{schema}'
            AND c.relname = '{table}'
            AND x.indisprimary = FALSE
        GROUP BY
            x.indexrelid
      )
SELECT
      n.nspname AS schema_name
    , c.relname AS table_name
    , i.relname AS index_name
    , pg_get_indexdef(i.oid) || ';' AS index_def
FROM
    pg_catalog.pg_index x
JOIN pg_catalog.pg_class c
    ON c.oid = x.indrelid
JOIN pg_catalog.pg_class i
    ON i.oid = x.indexrelid
JOIN pg_catalog.pg_namespace n
    ON n.oid = c.relnamespace
INNER JOIN index_column_names icn
    ON  icn.indexrelid = x.indexrelid
    AND ARRAY['{"','".join(cols)}'] @> icn.column_names
WHERE
        (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"]))
    AND (i.relkind = ANY (ARRAY['i'::"char", 'I'::"char"]))
    AND n.nspname = '{schema}'
    AND c.relname = '{table}'
    AND x.indisprimary = FALSE
    """
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            curs.execute(
                query,
                (),
            )
            return [dict(row) for row in curs.fetchall()]

def sort_input_columns(
    columns: List[str]
) -> Tuple[List[str], List[str]]:
    """Sort passed argument columns into start and end target column lists"""
    target_start: List[str] = []
    target_end: List[str] = []
    end_col = False
    for col in list(columns):
        # ... designates seperation from start to end target columns
        if col == "...":
            end_col = True
        elif end_col:
            target_end.append(col)
        else:
            target_start.append(col)
    return target_start, target_end


def reorder_columns(
    target_start: List[str],
    target_end: List[str],
    target_exclude: List[str],
    columns: List[str],
) -> List[str]:
    """Given a lost of columns and several target lists, return a sorted list of columns"""
    cols = [(col.split()[0], col) for col in columns]
    cols = [row for row in cols if row[0] not in target_exclude]
    start_cols = [row for row in cols if row[0] in target_start]
    start_cols.sort(key=lambda r: target_start.index(r[0]))
    all_cols = [
        row for row in cols if row[0] not in target_start and row[0] not in target_end
    ]
    end_cols = [row for row in cols if row[0] in target_end]
    end_cols.sort(key=lambda r: target_end.index(r[0]))
    result = [row[1] for row in start_cols + all_cols + end_cols]
    return result


def printcols(cols: List[str], header: Optional[str] = None) -> None:
    """Print columns with optional header text"""
    if header is not None:
        print(header)
    for row in cols:
        print(f"    {row}")


@click.command()
@click.option("--exclude", "-e", multiple=True, help="Exclude a column (can be used multiple times).")
@click.option("--host", "-h", default="localhost", help="The hostname of the Postgres server.")
@click.option("--port", "-p", default="5432", help="The port Postgres is listening on.")
@click.option("--database", "-d", required=True, help="The name of the database.")
@click.option("--schema", "-n", default="public", help="The schema of the target table.")
@click.option("--user", "-u", default="postgres", help="User name.")
@click.option("--password", "-p", default="", help="Password.")
@click.option("--migrate", "-m", is_flag=True, help="Output full migration sql.")
@click.option("--file", "-f", "output_file", type=click.File("w"), help="Write output into a file.")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main(
    migrate: bool,
    exclude,
    host: str,
    port: int,
    database: str,
    schema,
    user: str,
    password: str,
    output_file,
    table: str,
    columns: Tuple[str],
) -> None:
    """ Reorder PostgreSQL tables to match a target structure

    Columns are any number of arguments. When entered as "col1 col2 col3" the listed
    columns will be placed at the start of the table. When entered as
    "col1 col2 ... col3" the first two columns will be placed at the start of the table,
    and the last column will be placed at the end of the table. When entered as
    "... col1 col2 col3" all three columns will be placed at the end of the table.
    """
    cols, extras = get_columns(host, port, database, schema, table, user)

    if len(columns):
        target_start, target_end = sort_input_columns(list(columns))
        cols = reorder_columns(target_start, target_end, list(exclude), cols)

        if migrate:
            query = get_migration_sql(host, port, database, schema, table, user, password, cols, extras)

            if output_file is not None:
                output_file.write(query)
            else:
                print(query)
        else:
            printcols(cols, f"Ordered columns for {table}:")
    else:
        printcols(cols, f"Columns for {table}:",)


if __name__ == "__main__":
    main()
