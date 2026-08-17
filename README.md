# Biomedical Network Analysis with PySpark

This project uses PySpark to analyze biomedical data from the Hetionet network. The dataset includes different types of biological information, such as compounds, genes, diseases, and anatomy, and the relationships between them.
The goal of this project was to use large-scale data processing to explore these relationships and find useful patterns in the network.

## What I Did

* Analyzed the number of gene and disease connections for different compounds
* Looked at the distribution of compound connections across diseases
* Used relationships between genes and anatomy to find possible compound-disease connections that were not already in the dataset

## Tools

* Python
* PySpark
* Hetionet dataset

## Files

* `load_data.py` - Loads and prepares the Hetionet data
* `q1.py` - Analyzes compound-gene and compound-disease connections
* `q2.py` - Analyzes disease-compound connection distributions
* `q3.py` - Looks for possible new compound-disease relationships

## Dataset

The project uses node and edge data from Hetionet. The nodes represent biological entities such as compounds, genes, diseases, and anatomy, while the edges represent relationships between them.
