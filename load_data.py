from pyspark.sql import SparkSession

def load_tables():

    spark = SparkSession.builder.appName("Project2").getOrCreate()

    nodes = spark.read.option("sep", "\t") \
        .option("header", True) \
        .csv("nodes.tsv")

    edges = spark.read.option("sep", "\t") \
        .option("header", True) \
        .csv("edges.tsv")

    return spark, nodes, edges