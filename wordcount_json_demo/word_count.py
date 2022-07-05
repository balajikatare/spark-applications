### read the data from files and count total number of words

from pyspark.sql import SparkSession
import getpass
import json
from pyspark.sql.functions import split, explode, count, lit
props = json.load(open("wordcount.json"))
username = getpass.getuser()
appname = "{}| Word count".format(username)
spark = SparkSession.builder.appName(appname).master('yarn').getOrCreate()

input_dir = "{}/part-m-0000*".format(props["wordcount.input.dir"])
output_dir = "/user/itv002381/{}".format(props["wordcount.output.dir"])
lines = spark.read.text(input_dir).toDF("line")### lines dataframe have line column. each record is nothing but a sentance.For example: 'python is a easy ## language'

word_df = lines.select(split(lines.line,' ').alias("words"))### Split each line into list or array of words. For example: ['python', 'is', 'easy', 'language']
### now we need to convert each  list to record so that we can apply aggrigate functions

word_df = word_df.select(explode(word_df.words).alias("words"))## Now each list element will be the record
result = word_df.groupBy(word_df.words).agg(count(lit(1)).alias("word_count"))
result.write.mode("overwrite").csv(output_dir)