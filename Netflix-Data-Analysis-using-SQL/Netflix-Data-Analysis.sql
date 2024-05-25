-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Databases and Tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS NETFLIX

-- COMMAND ----------

USE DATABASE netflix

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC df = spark.read.csv("/FileStore/netflix_titles.csv", inferSchema=True, header=True)
-- MAGIC
-- MAGIC df = df.withColumn("date_added", to_date(df["date_added"], "MMMM d, yyyy")).withColumn(
-- MAGIC     "release_year", col("release_year").cast(IntegerType())
-- MAGIC )
-- MAGIC
-- MAGIC df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(
-- MAGIC     "netflix_data"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploratory Data Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Understanding the Data
-- MAGIC Let's start by understanding the structure of our Netflix data:

-- COMMAND ----------

select
  *
from
  netflix.netflix_data
limit
  5

-- COMMAND ----------

select
  count(1) as count
from
  netflix_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Type of Content
-- MAGIC How many TV shows and movies are available on Netflix?

-- COMMAND ----------

select
  type,
  count(1) as cnt
from
  netflix_data
where
  type in ('TV Show', 'Movie')
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Top 10 Directors
-- MAGIC Who are the top 10 directors with the most content on Netflix?

-- COMMAND ----------

select
  director,
  count(1) as content_cnt
from
  netflix_data
where
  director is not null
group by
  1
order by
  2 desc
limit
  10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Popular Genres
-- MAGIC What are the 5 most popular genres on Netflix?

-- COMMAND ----------

select
  genre,
  count(1) as cnt
from
  (
    select
      explode(split(listed_in, ',')) as genre
    from
      netflix_data
  )
group by
  1
order by
  2 desc
limit
  5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Recent Additions
-- MAGIC What are the 10 most recently added titles on Netflix?

-- COMMAND ----------

select
  title,
  date_added
from
  netflix_data
order by
  date_added desc
limit
  10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Average Release Year by Content Type
-- MAGIC Calculate the average release year for TV shows and movies separately.

-- COMMAND ----------

select
  type,
  cast(avg(release_year) as int) as average_release_year
from
  netflix_data
where
  type in ('TV Show', 'Movie')
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Top 10 Countries with the Most Content
-- MAGIC Identify the top 10 countries with the most content available on Netflix.

-- COMMAND ----------

select
  country,
  count(1) as content_count
from
  netflix_data
where
  country is not null
group by
  1
order by
  2 desc
limit
  10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. Longest and Shortest Duration
-- MAGIC Find the titles with the longest and shortest durations on Netflix.

-- COMMAND ----------

SELECT
    title,
    duration
FROM netflix_data
ORDER BY
    CASE WHEN duration LIKE '%h%' THEN CAST(SUBSTRING_INDEX(duration, 'h', 1) AS INTEGER) ELSE 0 END DESC,
    CASE WHEN duration LIKE '%m%' THEN CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'h', -1), 'm', 1) AS INTEGER) ELSE 0 END DESC
LIMIT 1;


-- COMMAND ----------

SELECT
    title,
    duration
FROM netflix_data
WHERE duration LIKE '%min'
ORDER BY
    CASE WHEN duration LIKE '%h%' THEN CAST(SUBSTRING_INDEX(duration, 'h', 1) AS INTEGER) ELSE 0 END ASC,
    CASE WHEN duration LIKE '%m%' THEN CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'h', -1), 'm', 1) AS INTEGER) ELSE 0 END ASC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 9. Content Added per Year
-- MAGIC Analyze how much content was added to Netflix each year.

-- COMMAND ----------

select
  YEAR(date_added) as added_year,
  count(1) as content_added
from
  netflix_data
where
  date_added is not NULL
group by
  1
order by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 10. Top 10 Longest Movie Titles
-- MAGIC Find the top 10 movies with the longest titles.

-- COMMAND ----------

select
  title,
  length(title) as length
from
  netflix_data
where
  type = 'Movie'
order by
  length desc
limit
  10
