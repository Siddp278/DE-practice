from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, size, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BigQuery Read") \
    .getOrCreate()


dataset_id = "DE_practice"
table_id = "floods-table"

# Read data from BigQuery
df = spark.read \
    .format("bigquery") \
    .option("dataset", dataset_id) \
    .option("table", table_id) \
    .load()

keywords = [
    "River", "Flooding", "Water", "Levels", "Land", "Roads", "Low-lying", "Footpaths", "Bridges", "Flood water",
    "Monitoring", "Dry", "Rainfall", "High", "Falling", "Storm", "Darragh", "Warning", "Areas", "Discharge",
    "Causeway", "Risk", "Danger", "Tributaries", "Floodplain", "Roads", "Tides", "Overflow", "Channels", "Rain",
    "Reservoir", "Protection", "Diversion", "Communities", "Rising", "Drains", "Brook", "Gauge", "Upstream",
    "Downstream", "Stagnant", "Sewage", "Evacuation", "Property", "Damage", "Environment", "Agency", "Hydrology",
    "Submergence", "Drainage", "Weedscreens", "Obstruction", "Pathways", "Culverts", "Debris", "Saturation",
    "Overflows", "Sediment", "Washes", "Tide lock", "Backwater", "Mooring", "Sluices", "Spillway", "Levees",
    "Bank", "Overflowing", "Crest", "Flash", "Drought", "Precipitation", "Watershed", "Erosion", "Subsidence",
    "Riverbanks", "Aquifers", "Gradient", "Streams", "Surface", "Inundation", "Pumping", "Dykes", "Dams",
    "Groundwater", "Floodgate", "Seepage", "Retention", "Detention", "Backflow", "Saturated", "Basin", "Channels",
    "Barriers", "Overflow", "Surge", "Scouring", "Flash flood", "Overflowing", "Alert"
]

df_with_word_counts = df.withColumn("words", split(col("message"), " ")) \
    .withColumn("total_word_count", size(col("words"))) \
    .withColumn(
        "certain_word_count",
        expr(
            f"size(filter(words, word -> word IN ({', '.join([repr(word) for word in keywords])})))"
        ),
    )

# Select required columns
result = df_with_word_counts.withColumn("rank_score", col("certain_word_count")/col("total_word_count"))
results = result.select("flood_id", "rank_score")

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "data-storage-unique"
spark.conf.set('temporaryGcsBucket', bucket)

# Save the data to BigQuery
results.write.format('bigquery') \
  .option('table',  + "DE_practice.floods-ranking") \
  .save()
