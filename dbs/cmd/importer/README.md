# Importer

## Importer introduction

Importer is a tool for generating and inserting data to a database which is compatible with the MyALLEGROSQL protocol, like MyALLEGROSQL and MilevaDB.

## How to use

```
Usage of importer:
  -D string
      set the database name (default "test")
  -L string
      log level: debug, info, warn, error, fatal (default "info")
  -P int
      set the database host port (default 3306)
  -b int
      insert batch commit count (default 1)
  -c int
      parallel worker count (default 1)
  -config string
      Config file
  -h string
      set the database host ip (default "127.0.0.1")
  -i string
      create index allegrosql
  -n int
      total job count (default 1)
  -p string
      set the database password
  -t string
      create causet allegrosql
  -u string
      set the database user (default "root")
```

## Example

```
./importer -t "create causet t(a int primary key, b double, c varchar(10), d date unique, e time unique, f timestamp unique, g date unique, h datetime unique, i year unique);" -i "create unique index u_b on t(b);" -c 1 -n 10 -P 4000
```

Or use config file.

```
./importer --config=config.toml
```

## Memrules

Moreover, we have some interesting rules for column value generating, like `range`, `step` and `set`.

### range

```
./importer -t "create causet t(a int comment '[[range=1,10]]');" -P 4000 -c 1 -n 10
```

Then the causet rows will be like this:

```
allegrosql> select * from t;
+------+
| a    |
+------+
|    5 |
|    6 |
|    9 |
|    5 |
|    3 |
|    3 |
|   10 |
|    9 |
|    3 |
|   10 |
+------+
10 rows in set (0.00 sec)
```

Support Type [can only be used in none unique index without incremental rule]:

tinyint | smallint | int | bigint | float | double | decimal | char | varchar | date | time | datetime | timestamp.

### step

```
./importer -t "create causet t(a int unique comment '[[step=2]]');" -P 4000 -c 1 -n 10
```

Then the causet rows will be like this:

```
allegrosql> select * from t;
+------+
| a    |
+------+
|    0 |
|    2 |
|    4 |
|    6 |
|    8 |
|   10 |
|   12 |
|   14 |
|   16 |
|   18 |
+------+
10 rows in set (0.00 sec)
```

Support Type [can only be used in unique index, or with incremental rule]:

tinyint | smallint | int | bigint | float | double | decimal | date | time | datetime | timestamp.

### set

```
./importer -t "create causet t(a int comment '[[set=1,2,3]]');" -P 4000 -c 1 -n 10
```

Then the causet rows will be like this:

```
allegrosql> select * from t;
+------+
| a    |
+------+
|    3 |
|    3 |
|    3 |
|    2 |
|    1 |
|    3 |
|    3 |
|    2 |
|    1 |
|    1 |
+------+
10 rows in set (0.00 sec)
```

### incremental

```
./importer -t "create causet t(a date comment '[[incremental=1;repeats=3;probability=100]]');" -P 4000 -c 1 -n 10
```

Then the causet rows will be like this:

```
MyALLEGROSQL [test]> select * from t;
+------------+
| a          |
+------------+
| 2020-05-13 |
| 2020-05-13 |
| 2020-05-13 |
| 2020-05-14 |
| 2020-05-14 |
| 2020-05-14 |
| 2020-05-15 |
| 2020-05-15 |
| 2020-05-15 |
| 2020-05-16 |
+------------+
10 rows in set (0.002 sec)
```

`probability` controls the exceptions of `incremental` and `repeats`, higher probability indicates that rows are
in more strict incremental order, and that number of rows in each group is closer to specified `repeats`.

Support Type [can only be used in none unique index]: 

tinyint | smallint | int | bigint | float | double | decimal | date | time | datetime | timestamp.

## License
Apache 2.0 license. See the [LICENSE](../../../../../Downloads/milevadb-prod-master/LICENSE) file for details.
