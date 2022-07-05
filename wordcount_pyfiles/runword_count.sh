export PYSPARK_PYTHON=python3
export SPARK_MAJOR_VERSION=3
spark-submit --master yarn --conf spark.conf.ui=0 /home/itv002381/wordcount/word_count.py