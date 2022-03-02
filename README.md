# Reorder Table Columns

Reorder PostgreSQL tables to match a target structure

When no columns are entered, the table's current columns are printed and the script returns.  
Columns  can be any number of arguments. When entered as "col1 col2 col3" the listed
columns will be placed at the start of the table. When entered as
"col1 col2 ... col3" the first two columns will be placed at the start of the table,
and the last column will be placed at the end of the table. When entered as
"... col1 col2 col3" all three columns will be placed at the end of the table.

### Options
```
-e, --exclude TEXT   Exclude a column (can be used multiple times).
-h, --host TEXT      The hostname of the Postgres server.
-p, --port TEXT      The port Postgres is listening on.
-d, --database TEXT  The name of the database.
-n, --schema TEXT    The schema of the target table.
-u, --user TEXT      User name.
-p, --password TEXT  Password.
-m, --migrate        Output full migration sql.
-f, --file FILENAME  Write output into a file.
-h, --help           Show this message and exit.
```

### Usage

Say we have the following table, `books`:

| author              | year_published | title                            | id |
|---------------------|----------------|----------------------------------|----|
| Mark Z. Danielewski | 2000           | House of Leaves                  | 1  |
| Harper Lee          | 1960           | To Kill a Mockingbird            | 2  |
| Douglas Adams       | 1979           | Hitchhiker's Guide to the Galaxy | 3  |

We want to move the `id` column to the start and the `year_published` column to the end. Simply run the following to get the query that will rearrange the table for you:

```sh
./reorder.py books id ... year_published
```

To run the sql, you can copy and paste it into a query, pipe it directly into the `psql` command, or save it to a file with the `--file` argument. Running the sql will result in the following table:

| id | author              | title                            | year_published |
|----|---------------------|----------------------------------|----------------|
| 1  | Mark Z. Danielewski | House of Leaves                  | 2000           |
| 2  | Harper Lee          | To Kill a Mockingbird            | 1960           |
| 3  | Douglas Adams       | Hitchhiker's Guide to the Galaxy | 1979           |

### Notes

Special thanks to JiCiT for [adding windows support and username/password options](https://github.com/TriangleCommunications/reorder-table-columns/pull/3)
