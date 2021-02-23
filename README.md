
# parquet_fdw

Parquet foreign data wrapper for PostgreSQL.

This is a fork, original at the [Adjust GitHub page](https://github.com/adjust/parquet_fdw).

## Installation

### Prerequisites

- PostgreSQL 11 or 12 + server development packages

- `libarrow` and `libparquet` 2.0.0 installed on your system.
  Compatibility with version 3.0.0 is not yet tested but might work.

- A compiler supporting C++17. If you are on a system where LLVM does not
  support C++17 (e.g. CentOS 7), run `make` with `with_llvm=0`.

### Building

```
make PG_CONFIG=<path-to-pg_config>
make PG_CONFIG=<path-to-pg_config> install
```

You can do a `make PG_CONFIG=<path-to-pg_config> PGUSER=<username> installcheck`
to run verification tests.


## Usage

To start using `parquet_fdw` you need to create the extension followed by
creating the server and user mapping. For example:

```sql
create extension parquet_fdw;
create server parquet_srv foreign data wrapper parquet_fdw;
create user mapping for postgres server parquet_srv options (user 'postgres');
```


### Creating foreign tables

Next you can create foreign tables pointing to parquet files. For instance:

```sql
CREATE FOREIGN TABLE foo(
    key BIGINT
  , value TEXT
) SERVER parquet_srv OPTIONS (
  filename '<path-to-parquet-file>'
);
```

The schema of the parquet file must match the table definition. Below is a list
of equivalent data types.


### Options

The following options are currently available:

- **filename**: the files to process, can be:
  - A single file, or
  - A space-separated list of files, or
  - A directory with parquet files. If a directory is provided, it is going to
    be processed recursively. Further, it is assumed that all files in there
    do have the same schema.


## Parallel querying

`parquet_fdw` also supports [parallel query execution](https://www.postgresql.org/docs/current/parallel-query.html).
To make use of parallel queries, you need to run `ANALYZE` on the foreign
table(s) to build statistics.


## Filter pushdown

On querying, `parquet_fdw` uses parquet statistics to calculate which row
groups need to be scanned thus effectively reducing the amount of data to read.


## Data types

Currently `parquet_fdw` supports the following column types:

| Parquet type |  SQL type |
|--------------|-----------|
|        INT32 |      INT4 |
|        INT64 |      INT8 |
|        FLOAT |    FLOAT4 |
|       DOUBLE |    FLOAT8 |
|    TIMESTAMP | TIMESTAMP |
|       DATE32 |      DATE |
|       STRING |      TEXT |
|       BINARY |     BYTEA |

`parquet_fdw` does not support array, structs and similar objects.


## Other features

### Import

`parquet_fdw` also supports [`IMPORT FOREIGN SCHEMA`](https://www.postgresql.org/docs/current/sql-importforeignschema.html)
to discover parquet files in the specified directory on filesystem and create
foreign tables accordingly. For example:

```sql
import foreign schema "/path/to/directory"
from server parquet_srv
into public;
```

It is important that `remote_schema` here is a path to a local filesystem
directory and is double quoted.

Another way to import parquet files into foreign tables is to use
`import_parquet` or `import_parquet_explicit`:

```sql
create function import_parquet(
    tablename   text,
    schemaname  text,
    servername  text,
    userfunc    regproc,
    args        jsonb,
    options     jsonb)
```

`userfunc` is a user-defined function. It must take a `jsonb` argument and return a text array of filesystem paths to parquet files to be imported. `args` is user-specified jsonb object that is passed to `userfunc` as its argument. A simple implementation of such function and its usage may look like this:

```sql
create function list_parquet_files(args jsonb)
returns text[] as
$$
begin
    return array_agg(args->>'dir' || '/' || filename)
           from pg_ls_dir(args->>'dir') as files(filename)
           where filename ~~ '%.parquet';
end
$$
language plpgsql;

select import_parquet(
    'abc',
    'public',
    'parquet_srv',
    'list_parquet_files',
    '{"dir": "/path/to/directory"}'
);
```

