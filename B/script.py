# This is not a script that'll run
# This is a log of the iPython notebook

#!/usr/bin/env python

# Boilerplate

from pyspark.sql import SQLContext
import itertools
from itertools import chain
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

sqlContext = SQLContext(sc)

# Parquet Creation
"""
df = spark.read.json("comments-minimal.json.bz2")
df.write.parquet("comments-minimal.parquet")
"""

sc.addPyFile("cleantext.py")
df = sqlContext.read.parquet("comments-minimal.parquet")

"""
comments = sqlContext.read.json("comments-minimal.json.bz2")
submissions = sqlContext.read.json("submissions.json.bz2")
"""

labeled = sqlContext.read.load("labeled_data.csv", format="csv", sep=":", inferSchema="true", header="true")

# TASK 2

# QUESTION 1:

# Check out some rows in the labeled dataframe
labeled.show()
"""
Functional Dependencies:
- Input.id ---> labeldem, labelgop, labeldjt
Which implies, by Armstrong axioms:
- Input.id ---> labeldem
- Input.id ---> labelgop
- Input.id ---> labeldjt
- Et al. by decomposition/association
"""

# QUESTION 2:

# Checkout the schema for comments dataframe
comments.printSchema()
"""
root
 |-- author: string (nullable = true)
 |-- author_cakeday: boolean (nullable = true)
 |-- author_flair_css_class: string (nullable = true)
 |-- author_flair_text: string (nullable = true)
 |-- body: string (nullable = true)
 |-- can_gild: boolean (nullable = true)
 |-- can_mod_post: boolean (nullable = true)
 |-- collapsed: boolean (nullable = true)
 |-- collapsed_reason: string (nullable = true)
 |-- controversiality: long (nullable = true)
 |-- created_utc: long (nullable = true)
 |-- distinguished: string (nullable = true)
 |-- edited: string (nullable = true)
 |-- gilded: long (nullable = true)
 |-- id: string (nullable = true)
 |-- is_submitter: boolean (nullable = true)
 |-- link_id: string (nullable = true)
 |-- parent_id: string (nullable = true)
 |-- permalink: string (nullable = true)
 |-- retrieved_on: long (nullable = true)
 |-- score: long (nullable = true)
 |-- stickied: boolean (nullable = true)
 |-- subreddit: string (nullable = true)
 |-- subreddit_id: string (nullable = true)
 |-- subreddit_type: string (nullable = true)

"""

# Checkout a row in the dataframe
comments.show(n=1)
"""
+----------+--------------+----------------------+-----------------+--------------------+--------+------------+---------+----------------+----------------+-----------+-------------+------+------+-------+------------+---------+----------+---------+------------+-----+--------+---------+------------+--------------+
|    author|author_cakeday|author_flair_css_class|author_flair_text|                body|can_gild|can_mod_post|collapsed|collapsed_reason|controversiality|created_utc|distinguished|edited|gilded|     id|is_submitter|  link_id| parent_id|permalink|retrieved_on|score|stickied|subreddit|subreddit_id|subreddit_type|
+----------+--------------+----------------------+-----------------+--------------------+--------+------------+---------+----------------+----------------+-----------+-------------+------+------+-------+------------+---------+----------+---------+------------+-----+--------+---------+------------+--------------+
|-0rabbit0-|          null|                  null|             null|It's not *somethi...|    null|        null|     null|            null|               0| 1482456914|         null| false|     0|dbj1fux|        null|t3_5jsgsc|t1_dbiy5l7|     null|  1483978698|    2|   false| politics|    t5_2cneq|          null|
+----------+--------------+----------------------+-----------------+--------------------+--------+------------+---------+----------------+----------------+-----------+-------------+------+------+-------+------------+---------+----------+---------+------------+-----+--------+---------+------------+--------------+
only showing top 1 row
"""

# Is it normalized?
"""
There are 3 distinct entities involved in the table:
- sub-reddit, user, comment

The comment id is candidate key of entire relationship.
User data like flair, cake etc. has partial dependency on author
Subreddit data like sub name has partial dependency on the s_id

So, considering basic 2NF criteria:
- Table is not normalized.
- As a starter, can be decomposed into distinct comment, user and sub-reddit tables.
- Keyed by id, author and s-id attributes respectively.

Consider comment data:
- Candidate key is id + any trivial superset
- Functional Dependencies:
    - id -> all attributes (by def of key)
    - body -> link_id (maybe, kinda extra tbh) i.e. a transitive dependency that can be decomposed in 3NF

Why is given table not normalized?
- Typically, every rendering of a comment on a reddit page uses all the attributes in the non normalized table
- If normalized, we'll need to compute an expensive JOIN for each useful render of a comment
- So even though there is redundancy that can be removed, the data makes sense collectively - particularly on the front-end.
"""

# READ: https://docs.databricks.com/spark/latest/spark-sql/udf-in-python.html

# TASK 4 & 5
"""
Spark UDF for n-gram generation on the comment body...
"""
from cleantext import sanitize

def doStuff(grams):
    res = []
    for gram in grams:
        for str in gram.split():
            res.append(str)
    return res

f = udf(lambda x: doStuff(sanitize(x)[1:]), ArrayType(StringType()));
data = df.select('*', f('body').alias('grams'))

# TODO: TASK 6: PROBABLY THE HARDEST PART
"""
PIPELINE
1. CountVectorizer to turn raw features into Spark ML feature vector
2. StringIndexer
"""
