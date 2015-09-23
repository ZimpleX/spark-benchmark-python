## These are benchmarks for [Spark](https://github.com/apache/spark/) (in python)
* The characteristics of these benchmarks are heavy MapReduce operations, including:
    * Logistic Regression in neural network training
    * ...

## Content
* `submit-to-spark.sh`: wrapper of `spark-submit` script inside `spark/bin` dir
    * renaming existing log files: as `1.out`, `2.out`, ... `n.out` in chronological order
    * all log files are read-only: ensure that chronological orde won't mess up
    * submit via `spark-submit`, write new log to `(n+1).out`
    * **_TODO_**: echo revision number of benchmark to log file
    * **_TODO_**: add arg parsing to support general benchmark testing
* `benchmark-*` : directories containing all source code for different benchmarks

## Dir structure
* this repo (`benchmark/` in the following graph) and the `Spark` directory share the same parent directory
* the log file generated by `submit-to-spark.sh` is stored in `bm-log/<benchmark-name>/`
```
project
|
`-- benchmark-python/
|   |
|   `-- LogReg/
|   |
|   `-- ...
|
`-- spark-1.5.0/
|   |
|   `-- ...
|
`-- bm-log/
    |
    `-- LogReg/
    |   |
    |   `-- 1.out
    |   |
    |   `-- 2.out
    `-- ...
```
