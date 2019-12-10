# master-thesis: Design und Implementation of a Data Lake Architecture for Analyses based on Open Data

This is my (un)official repository for my master thesis. Its about how to organize, process and analyze (open) data in a data lake architecure. the processes and analyses in a data lake were practically implemented. the use case considers an optimization of retail store locations in germany. For this purpose DWH data and external population and infrastructure data from Open Data are used and migrated on a Hadoop Cluster. The performance forecast were implemented with a neuronal network (Keras/Tensorflow) and Random Forest (SparkML).

There are two section. "docs" contains the written elaboration including presentation of results. in the "data lake" folder you will find the data and scripts in the following structure:

0_landing - stores raw data and metadata of each data source
1_landing - stores standardized data (in format and structure) and metadata for each data source
2_curated - stores cleansed data and Python code (Jupyter Notebooks) for each data source
3_working - follows a application specific structure. each application dir contains code and transformed & joined data. Here you can also find the implementation of the machine learning algorithms


Highlights:
- methods for handling missing values (time-dependent and time-independent)
- merging geographical data and optimize runtime with Spark techniques
- comparison of two machine learning algos
