# Databricks notebook source
# MAGIC %run /Users/shailij@openai.com/startup_notebook

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("HiveToPandas").enableHiveSupport().getOrCreate()

# COMMAND ----------

hive_table = "hive_metastore.test.sextortion_investigation"
df_spark = spark.sql(f"SELECT * FROM {hive_table} WHERE ds='2024-04-04'")

# COMMAND ----------

c4_df = df_spark.toPandas()
c4_df.head()

# COMMAND ----------

print(c4_df.columns)

# COMMAND ----------

blackmail_extort_df = c4_df[c4_df["ind_blackmail_extort"] == '1']
print(len(blackmail_extort_df.index))

# COMMAND ----------

display(blackmail_extort_df)

# COMMAND ----------

blackmail_extort_df.iloc[28]["raw_content"]

# COMMAND ----------

blackmail_df = c4_df[c4_df["ind_advice_blackmail"] == '1']
print(len(blackmail_df.index))

# COMMAND ----------

display(blackmail_df)

# COMMAND ----------

sextortion_df = c4_df[c4_df["ind_advice_sextortion"] == '1']
print(len(sextortion_df.index))

# COMMAND ----------

display(sextortion_df)

# COMMAND ----------

sextortion_df.iloc[1]["raw_content"]

# COMMAND ----------

adv_df = c4_df[c4_df["ind_adversarial_prompt"] == '1']
print(len(adv_df.index))

# COMMAND ----------

jb_df = c4_df[c4_df["ind_jailbreak"] == '1']
print(len(jb_df.index))

# COMMAND ----------

sm_df = c4_df[c4_df["ind_social_media"] == '1']
print(len(sm_df.index))

# COMMAND ----------

display(sm_df)

# COMMAND ----------

flirtation_df = c4_df[c4_df["ind_flirtation"] == '1']
print(len(flirtation_df.index))

# COMMAND ----------

election_false_fabrication_df = curr_elec_df[curr_elec_df["ind_election_false_fabrication"] == '1']
print(len(election_false_fabrication_df.index))

# COMMAND ----------

display(election_false_fabrication_df)

# COMMAND ----------

sexual_content_df = c4_df[c4_df["ind_sexual_content"] == '1']
print(len(sexual_content_df.index))

# COMMAND ----------

nude_photo_df = c4_df[c4_df["ind_nude_photo"] == '1']
print(len(nude_photo_df.index))

# COMMAND ----------

photo_df = c4_df[c4_df["ind_photo"] == '1']
print(len(photo_df.index))

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
