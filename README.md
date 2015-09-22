## This is benchmarks for [Spark](https://github.com/apache/spark/)
* The characteristics of these benchmarks are heavy MapReduce operations, including:
    * Logistic Regression in neural network training
    * ...

## Dir structure
* `submit-to-spark.sh`: wrapper of `spark-submit` script inside `Spark` dir
    * renaming existing log files: as `1.out`, `2.out`, ... `n.out` in chronological order
    * all log files are read-only: ensure that chronological orde won't mess up
    * submit via `spark-submit`, write new log to `(n+1).out`
    * **_TODO_**: echo revision number of benchmark to log file
* `benchmark-*` : directories containing all source code for different benchmarks
