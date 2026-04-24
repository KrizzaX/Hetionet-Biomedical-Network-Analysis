from load_data import load_tables
from pyspark.sql.functions import col, countDistinct, desc

spark, nodes, edges = load_tables()

drug_nodes = nodes.filter(col("kind") == "Compound") \
    .select(
        col("id").alias("drug_id"),
        col("name").alias("drug_name")
    )

gene_nodes = nodes.filter(col("kind") == "Gene") \
    .select(col("id").alias("gene_id"))


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
    drug_nodes["drug_name"],
    edges["target"].alias("gene_id")
)

# Counts distinct genes for each drug
drug_gene_counts = drug_gene_edges.groupBy("drug_id", "drug_name") \
    .agg(countDistinct("gene_id").alias("num_genes"))

# Gets the top 5 drug names by number of genes
top5_q3 = drug_gene_counts.orderBy(desc("num_genes")).limit(5)

print("Q3: Top 5 drug names by number of genes:")
top5_q3.show(truncate=False)

top5_q3.write.mode("overwrite").option("header", True).csv("q3_top5_results")

spark.stop()