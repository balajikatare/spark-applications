export PYSPARK_PYTHON=python3
export SPARK_MAJOR_VERSION=3
spark-submit --master yarn --deploy-mode client --properties-file /home/itv002381/wordcount_json_demo/wordcount.json --conf spark.conf.ui=0 /home/itv002381/wordcount_json_demo/word_count.py
