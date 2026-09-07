"""Word count in PySpark, with the same rules as the Hadoop job from Hands-on L4:
split on whitespace, ignore words shorter than three characters, sort by count.

Usage:
    spark-submit wordcount.py <input file> [<output directory>]

The output directory must not already exist. If it is omitted, the result is only printed.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, length, col

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(2)

spark = SparkSession.builder.appName("WordCount").getOrCreate()

lines = spark.read.text(sys.argv[1])
words = lines.select(explode(split(col("value"), r"\s+")).alias("word"))
counts = (words.filter(length("word") >= 3)
               .groupBy("word").count()
               .orderBy(col("count").desc(), col("word")))

counts.show(50, truncate=False)
print(f"{counts.count()} distinct words")

if len(sys.argv) > 2:
    # One partition so that the result is a single file, like the L4 part-r-00000.
    (counts.coalesce(1)
           .selectExpr("concat(word, ' ', count) AS line")
           .write.text(sys.argv[2]))
    print(f"Result written to {sys.argv[2]}")

spark.stop()
