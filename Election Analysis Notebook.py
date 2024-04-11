# Databricks notebook source
# MAGIC %run /Users/shailij@openai.com/startup_notebook

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("HiveToPandas").enableHiveSupport().getOrCreate()

# COMMAND ----------

hive_table = "hive_metastore.test.in_election_prompts"
df_spark = spark.sql(f"SELECT * FROM {hive_table} WHERE ds='2024-04-07'")

# COMMAND ----------

curr_elec_df = df_spark.toPandas()
curr_elec_df.head()

# COMMAND ----------

print(curr_elec_df.columns)

# COMMAND ----------

adv_df = curr_elec_df[curr_elec_df["ind_adversarial_prompt"] == '1']
print(len(adv_df.index))

# COMMAND ----------

display(adv_df)

# COMMAND ----------

jb_df = curr_elec_df[curr_elec_df["ind_jailbreak"] == '1']
print(len(jb_df.index))

# COMMAND ----------

display(jb_df)

# COMMAND ----------

jb_df.iloc[3]["raw_content"]

# COMMAND ----------

election_opinion_df = curr_elec_df[curr_elec_df["ind_election_opinion"] == '1']
print(len(election_opinion_df.index))

# COMMAND ----------

display(election_opinion_df)

# COMMAND ----------

election_false_information_df = curr_elec_df[curr_elec_df["ind_election_false_information"] == '1']
print(len(election_false_information_df.index))

# COMMAND ----------

display(election_false_information_df)

# COMMAND ----------

election_false_fabrication_df = curr_elec_df[curr_elec_df["ind_election_false_fabrication"] == '1']
print(len(election_false_fabrication_df.index))

# COMMAND ----------

display(election_false_fabrication_df)

# COMMAND ----------

election_tampering_df = curr_elec_df[curr_elec_df["ind_election_tampering_election"] == '1']
print(len(election_tampering_df.index))

# COMMAND ----------

display(election_tampering_df)

# COMMAND ----------

election_how_to_tamper_df = curr_elec_df[curr_elec_df["ind_election_how_to_tamper_election"] == '1']
print(len(election_how_to_tamper_df.index))

# COMMAND ----------

display(election_how_to_tamper_df)

# COMMAND ----------

election_voting_process_df = curr_elec_df[curr_elec_df["ind_election_voting_process"] == '1']
print(len(election_voting_process_df.index))

# COMMAND ----------

display(election_voting_process_df)

# COMMAND ----------

election_how_to_voting_process_df = curr_elec_df[curr_elec_df["ind_election_how_to_voting_process"] == '1']
print(len(election_how_to_voting_process_df.index))

# COMMAND ----------

election_social_media_df = curr_elec_df[curr_elec_df["ind_election_social_media"] == '1']
print(len(election_social_media_df.index))

# COMMAND ----------

display(election_social_media_df)

# COMMAND ----------

election_how_to_social_media_df = curr_elec_df[curr_elec_df["ind_election_how_to_social_media"] == '1']
print(len(election_how_to_social_media_df.index))

# COMMAND ----------

display(election_how_to_social_media_df)
