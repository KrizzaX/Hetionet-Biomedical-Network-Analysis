from load_data import load_tables
from pyspark.sql.functions import col, countDistinct, desc

spark, nodes, edges = load_tables()

# Get drug and disease IDs
drug_nodes = nodes.filter(col("kind") == "Compound") \
                  .select(col("id").alias("drug_id"))

disease_nodes = nodes.filter(col("kind") == "Disease") \
                     .select(col("id").alias("disease_id"))

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

# For each disease, counts how many distinct drugs are associated with it
disease_drug_counts = drug_disease_edges.groupBy("disease_id") \
    .agg(countDistinct("drug_id").alias("num_drugs"))

# Counts how many diseases are associated with a specific # of drugs
q2_result = disease_drug_counts.groupBy("num_drugs") \
    .agg(countDistinct("disease_id").alias("num_diseases"))

# Top 5 by number of diseases descending
top5_q2 = q2_result.orderBy(desc("num_diseases")).limit(5)

print("Top 5 Q2 results:")
top5_q2.show(truncate=False)

top5_q2.write.mode("overwrite").option("header", True).csv("q2_top5_results")

spark.stop()