from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc

spark = SparkSession.builder.appName("Project2_Q1").getOrCreate()

# Load files
nodes = spark.read.option("sep", "\t").option("header", True).csv("nodes.tsv")
edges = spark.read.option("sep", "\t").option("header", True).csv("edges.tsv")

# Get node IDs by type
drug_nodes = nodes.filter(col("kind") == "Compound").select(col("id").alias("drug_id"))
gene_nodes = nodes.filter(col("kind") == "Gene").select(col("id").alias("gene_id"))
disease_nodes = nodes.filter(col("kind") == "Disease").select(col("id").alias("disease_id"))

# Drug -> Gene edges
drug_gene_edges = edges.join(
    drug_nodes,
    edges["source"] == drug_nodes["drug_id"],
    "inner"
).join(
    gene_nodes,
    edges["target"] == gene_nodes["gene_id"],
    "inner"
).select(
    edges["source"].alias("drug_id"),
    edges["target"].alias("gene_id")
)

# Drug -> Disease edges
drug_disease_edges = edges.join(
    drug_nodes,
    edges["source"] == drug_nodes["drug_id"],
    "inner"
).join(
    disease_nodes,
    edges["target"] == disease_nodes["disease_id"],
    "inner"
).select(
    edges["source"].alias("drug_id"),
    edges["target"].alias("disease_id")
)

# Count distinct genes per drug
gene_counts = drug_gene_edges.groupBy("drug_id") \
    .agg(countDistinct("gene_id").alias("num_genes"))

# Count distinct diseases per drug
disease_counts = drug_disease_edges.groupBy("drug_id") \
    .agg(countDistinct("disease_id").alias("num_diseases"))

# Combine counts
q1_result = gene_counts.join(disease_counts, on="drug_id", how="outer") \
    .na.fill(0, subset=["num_genes", "num_diseases"])

# Top 5 drugs by gene count descending
top5_q1 = q1_result.orderBy(desc("num_genes")).limit(5)

print("All Q1 results:")
q1_result.show(truncate=False)

print("Top 5 drugs by number of genes:")
top5_q1.show(truncate=False)

q1_result.write.mode("overwrite").option("header", True).csv("q1_all_results")
top5_q1.write.mode("overwrite").option("header", True).csv("q1_top5_results")

spark.stop()