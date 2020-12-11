#!/usr/bin/env python3

import re
import subprocess
from inspect import cleandoc
from typing import List, Tuple, Optional

import click
import psycopg2
import psycopg2.extras


def get_dump_sql(database: str, schema: str, table: str) -> str:
    """Get SQL that would be returned with `pg_dump`"""
    result = subprocess.run(
        ["pg_dump", "--schema-only", f"--table={schema}.{table}", database],
        capture_output=True,
        check=True,
    )
    return result.stdout.decode()


def get_columns(sql_text: str, schema: str, table: str) -> Tuple[List[str], List[str]]:
    """Get columns for a table"""
    table_re = re.compile(
        fr"(?P<pre>(?:\n|.)+CREATE TABLE {schema}.{table}\s+\(\n)(?P<rows>(?:\n|.)+?)(?P<post>\);(?:\n|.)+)"
    )
    match = table_re.search(sql_text)
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
    sql_text: str, schema: str, table: str, columns: List[str], extras: List[str]
) -> str:
    """Get SQL command to migrate a source table into the target table"""
    sql_text = get_dump_sql(database, schema, table)
    table_re = re.compile(
        fr"(?P<pre>(?:\n|.)+)(?P<table>CREATE TABLE {schema}\.{table}\s+\(\n(?:\n|.)+?\);)(?P<post>(?:\n|.)+)"
    )
    match = table_re.search(sql_text)

    fk_disable = "\n".join(
        [
            cleandoc(
                f"""
            ALTER TABLE {fk['schema']}.{fk['local_table']}
            DROP CONSTRAINT {fk['constraint']};
        """
            )
            for fk in get_foreign_keys(database, schema, table)
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
            for fk in get_foreign_keys(database, schema, table)
        ]
    )
    extra_features = "\n".join(
        [
            f"ALTER TABLE {schema}.{table} ADD {row};"
            for row in extras
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
-- Disable foreign keys and drop old table
{fk_disable}
DROP TABLE {schema}.{table};
-- Rename new table
ALTER TABLE {schema}.{table}_migration RENAME TO {table};
-- Add extra features (constraints) back
{extra_features}
{match.group("post")}
-- Add foreign keys back
{fk_enable}
        """
    )


def get_foreign_keys(database: str, schema: str, table: str) -> List[dict]:
    """Get foreign keys referencing a given table"""
    with psycopg2.connect(database=database, user="postgres") as conn:
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
                AND ccu.table_name = %s
            """,
                (table,),
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
    target_exclude: Tuple[str],
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


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--exclude", "-e", multiple=True, help="Exclude a column (can be used multiple times).")
@click.option("--database", "-d", help="The name of the database.")
@click.option("--schema", "-n", default="public", help="The schema of the target table.")
@click.option("--migrate", "-m", is_flag=True, help="Output full migration sql.")
@click.option("--input-file", "-i", "input_file", type=click.File("r"), help="Write output into a file.")
@click.option("--output-file", "-o", "output_file", type=click.File("w"), help="Write output into a file.")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main(
    migrate: bool,
    exclude: Tuple[str],
    database: str,
    schema: str,
    input_file,
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

    sql_text = get_dump_sql(database, schema, table) if input_file is None else input_file.read()
    cols, extras = get_columns(schema, table, sql_text)

    if len(columns):
        target_start, target_end = sort_input_columns(list(columns))
        cols = reorder_columns(
            target_start,
            target_end,
            exclude,
            cols
        )

        if migrate:
            query = get_migration_sql(database, schema, table, cols, extras)

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
