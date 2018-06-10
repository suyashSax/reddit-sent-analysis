# This is not a script that'll run
# This is a log of the iPython notebook

#!/usr/bin/env python

# TASK 1

from pyspark.sql import SQLContext
import itertools
from itertools import chain
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

sqlContext = SQLContext(sc)

sc.addPyFile("cleantext.py")
comments = sqlContext.read.parquet("comments-minimal.parquet")
submissions = sqlContext.read.parquet("submissions.parquet")

model = CountVectorizerModel.load('countvec.model')
posModel = CrossValidatorModel.load('pos.model')
negModel = CrossValidatorModel.load('neg.model')


df = comments
# Parquet Creation
"""
df = spark.read.json("comments-minimal.json.bz2")
df.write.parquet("comments-minimal.parquet")
comments = sqlContext.read.json("comments-minimal.json.bz2")
submissions = sqlContext.read.json("submissions.json.bz2")
"""

labeled = sqlContext.read.load("labeled_data.csv", format="csv", sep=",", inferSchema="true", header="true")
labeled =  labeled.toDF("id", "dem", "gop", "trump")

# TASK 4 & 5
joined = df.join(labeled, ["id"])

from cleantext import faster

# def doStuff(grams):
#     res = []
#     for gram in grams:
#         for str in gram.split():
#             if str is not None:
#                 res.append(str)
#     return res
#
# f = udf(lambda x: doStuff(sanitize(x)[1:]), ArrayType(StringType()));
# data = joined.select('*', f('body').alias('grams'))

f = udf(lambda x: faster(x), ArrayType(StringType()));
data = joined.select('*', f('body').alias('grams'))

# TASK 6
cv = CountVectorizer(inputCol="grams", outputCol="feature", minDF=5.0, binary=True, vocabSize=1<<18)
model = cv.fit(data)
result = model.transform(data)

# Checkpoint It
# result.write.parquet("result.parquet")

# Read from checkpointed Parquet
result = sqlContext.read.parquet("result.parquet")

# Functions to return 1 or 0 for a label score
def negative_udf(label):
    if(label == -1):
        return 1
    return 0

def positive_column(label):
    if(label == 1):
        return 1
    return 0

# Add negative column to result
neg_func = udf(lambda x: negative_udf(x), IntegerType())
pos_func = udf(lambda x: positive_column(x), IntegerType())

negative = result.select('*', neg_func('trump').alias('negative'))
positive_negative = negative.select('*', pos_func('trump').alias('positive'))

# process Gop and Dem columns for extra credit
dem_negative = result.select('*', neg_func('dem').alias('negative'))
dem_positive_negative = dem_negative.select('*', pos_func('dem').alias('positive'))

gop_negative = result.select('*', neg_func('gop').alias('negative'))
gop_positive_negative = gop_negative.select('*', pos_func('gop').alias('positive'))


# neg = positive_negative.where(positive_negative['negative']==1)
# pos = positive_negative.where(positive_negative['positive']==1)

neg = positive_negative.withColumnRenamed('negative', 'label')
pos = positive_negative.withColumnRenamed('positive', 'label')

dem_neg = dem_positive_negative.withColumnRenamed('negative', 'label')
dem_pos = dem_positive_negative.withColumnRenamed('positive', 'label')

gop_neg = gop_positive_negative.withColumnRenamed('negative', 'label')
gop_pos = gop_positive_negative.withColumnRenamed('positive', 'label')


# TASK 7
poslr = LogisticRegression(labelCol="label", featuresCol="feature", maxIter=10)
neglr = LogisticRegression(labelCol="label", featuresCol="feature", maxIter=10)

posEvaluator = BinaryClassificationEvaluator()
negEvaluator = BinaryClassificationEvaluator()

posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()

posCrossval = CrossValidator(
    estimator=poslr,
    evaluator=posEvaluator,
    estimatorParamMaps=posParamGrid,
    numFolds=2)
negCrossval = CrossValidator(
    estimator=neglr,
    evaluator=negEvaluator,
    estimatorParamMaps=negParamGrid,
    numFolds=2)

posTrain, posTest = pos.randomSplit([0.5, 0.5])
negTrain, negTest = neg.randomSplit([0.5, 0.5])

dem_posTrain, dem_posTest = dem_pos.randomSplit([0.5, 0.5])
dem_negTrain, dem_negTest = dem_neg.randomSplit([0.5, 0.5])

gop_posTrain, gop_posTest = gop_pos.randomSplit([0.5, 0.5])
gop_negTrain, gop_negTest = gop_neg.randomSplit([0.5, 0.5])

print("Training positive classifier...")
posModel = posCrossval.fit(posTrain)

print("Training negative classifier...")
negModel = negCrossval.fit(negTrain)

print("Training dem positive classifier...")
dem_posModel = posCrossval.fit(dem_posTrain)

print("Training dem negative classifier...")
dem_negModel = negCrossval.fit(dem_negTrain)

print("Training gop positive classifier...")
gop_posModel = posCrossval.fit(gop_posTrain)

print("Training gop negative classifier...")
gop_negModel = negCrossval.fit(gop_negTrain)

posModel.save("pos.model")
negModel.save("neg.model")

dem_posModel.save("dem_pos.model")
dem_negModel.save("dem_neg.model")

gop_posModel.save("gop_pos.model")
gop_negModel.save("gop_neg.model")

# TASK 8
min_df = comments.select('id', 'link_id', 'created_utc', 'body', 'author_flair_text', 'score')
remove_t3_ = udf(lambda x: x[3:], StringType())

min_df = min_df.select('*', remove_t3_('link_id').alias('link_id_new'))
min_df = min_df.drop('link_id')
min_df = min_df.selectExpr("id as id", "created_utc as utc_created", "body as body", "author_flair_text as state", "link_id_new as link_id", "score as comment_score")

submissions = submissions.withColumnRenamed('id', 'link_id')
joined_2 = min_df.join(submissions, ["link_id"])

df8 = joined_2.select('id', 'title', 'link_id', 'utc_created', 'body', 'state', 'score', 'comment_score')

df8 = df8.withColumnRenamed('utc_created', 'created_utc')
df8 = df8.withColumnRenamed('state', 'author_flair_text')
df8 = df8.withColumnRenamed('score', 'story_score')

# TASK 9
df8 = df8.where(df8["body"][0:3] != "&gt")
df8 = df8.where(df8["body"].contains("/s") == False)

# Repeat task 4, 5 and 6A

# MAKE THE SAMPLE HERE
sample = df8.sample(False, 0.2, None)

df9 = sample.select('*', f('body').alias('grams'))
r9 = model.transform(df9)

def posProb(x):
    if x[1] > 0.2:
        return 1
    else:
        return 0

def negProb(x):
    if x[1] > 0.25:
        return 1
    else:
        return 0

posFunc = udf(lambda x: posProb(x), IntegerType())
negFunc = udf(lambda x: negProb(x), IntegerType())

res = posModel.transform(r9)
res = res.select('*', posFunc('probability').alias('pos'))
res = res.drop('probability','prediction', 'rawPrediction', 'grams')

res = negModel.transform(res)
res = res.select('*', negFunc('probability').alias('neg'))
res = res.drop('probability','prediction', 'rawPrediction', 'feature')

res.write.parquet('final.parquet')

# DEM and GOP
res_dem = dem_posModel.transform(r9)
res_dem = res_dem.select('*', posFunc('probability').alias('pos'))
res_dem = res_dem.drop('probability','prediction', 'rawPrediction', 'grams')

res_dem = dem_negModel.transform(res_dem)
res_dem = res_dem.select('*', negFunc('probability').alias('neg'))
res_dem = res_dem.drop('probability','prediction', 'rawPrediction', 'feature')

res_dem.write.parquet('final_dem.parquet')

res_gop = gop_posModel.transform(r9)
res_gop = res_gop.select('*', posFunc('probability').alias('pos'))
res_gop = res_gop.drop('probability','prediction', 'rawPrediction', 'grams')

res_gop = gop_negModel.transform(res_gop)
res_gop = res_gop.select('*', negFunc('probability').alias('neg'))
res_gop = res_gop.drop('probability','prediction', 'rawPrediction', 'feature')

res_gop.write.parquet('final_gop.parquet')

#TASK 10

task10_df = sqlContext.read.parquet("try5.parquet")

sqlContext.registerDataFrameAsTable(task10, "task10")

part_a = sqlContext.sql('select 100*avg(pos), 100*avg(neg) from task10')
part_a.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_a.csv")

part_b= sqlContext.sql('select 100*avg(pos), 100*avg(neg), DAYOFYEAR(FROM_UNIXTIME(created_utc)) from task10 group by DAYOFYEAR(FROM_UNIXTIME(created_utc)) ORDER BY DAYOFYEAR(FROM_UNIXTIME(created_utc))')
part_b.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_b.csv")

part_c = sqlContext.sql('select author_flair_text AS state,  100*avg(pos), 100*avg(neg) from task10 where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_c.csv")


part_d_by_comment_score = sqlContext.sql('select 100*avg(pos), 100*avg(neg), comment_score from task10 GROUP BY comment_score')
part_d_by_comment_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_comment_score.csv")

part_d_by_story_score = sqlContext.sql('select 100*avg(pos), 100*avg(neg), story_score from task10 GROUP BY story_score')
part_d_by_story_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_story_score.csv")

# Question 3
# Chose join from task 8: joined_2 = min_df.join(submissions, ["link_id"])
joined_2_explain = min_df.join(submissions, ["link_id"]).explain()

"""
== Physical Plan ==
Project [link_id#36222, id#74, utc_created#36219L, body#64, state#36221, comment_score#36223L, archived#110, author#111, author_cakeday#112, author_flair_css_class#113, author_flair_text#114, brand_safe#115, can_gild#116, can_mod_post#117, contest_mode#118, created_utc#119L, crosspost_parent#120, crosspost_parent_list#121, distinguished#122, domain#123, downs#124L, edited#125, gilded#126L, hidden#127, ... 41 more fields]
+- SortMergeJoin [link_id#36222], [link_id#36230], Inner
   :- *(4) Sort [link_id#36222 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(link_id#36222, 200)
   :     +- *(3) Project [id#74, created_utc#70L AS utc_created#36219L, body#64, author_flair_text#63 AS state#36221, pythonUDF0#37029 AS link_id#36222, score#80L AS comment_score#36223L]
   :        +- BatchEvalPython [<lambda>(link_id#76)], [author_flair_text#63, body#64, created_utc#70L, id#74, link_id#76, score#80L, pythonUDF0#37029]
   :           +- *(2) Project [author_flair_text#63, body#64, created_utc#70L, id#74, link_id#76, score#80L]
   :              +- *(2) Filter isnotnull(pythonUDF0#37028)
   :                 +- BatchEvalPython [<lambda>(link_id#76)], [author_flair_text#63, body#64, created_utc#70L, id#74, link_id#76, score#80L, pythonUDF0#37028]
   :                    +- *(1) FileScan parquet [author_flair_text#63,body#64,created_utc#70L,id#74,link_id#76,score#80L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/home/cs143/data/comments-minimal.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<author_flair_text:string,body:string,created_utc:bigint,id:string,link_id:string,score:big...
   +- Sort [link_id#36230 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(link_id#36230, 200)
         +- Project [archived#110, author#111, author_cakeday#112, author_flair_css_class#113, author_flair_text#114, brand_safe#115, can_gild#116, can_mod_post#117, contest_mode#118, created_utc#119L, crosspost_parent#120, crosspost_parent_list#121, distinguished#122, domain#123, downs#124L, edited#125, gilded#126L, hidden#127, hide_score#128, id#129 AS link_id#36230, is_crosspostable#130, is_reddit_media_domain#131, is_self#132, is_video#133, ... 36 more fields]
            +- Filter isnotnull(id#129)
               +- FileScan parquet [archived#110,author#111,author_cakeday#112,author_flair_css_class#113,author_flair_text#114,brand_safe#115,can_gild#116,can_mod_post#117,contest_mode#118,created_utc#119L,crosspost_parent#120,crosspost_parent_list#121,distinguished#122,domain#123,downs#124L,edited#125,gilded#126L,hidden#127,hide_score#128,id#129,is_crosspostable#130,is_reddit_media_domain#131,is_self#132,is_video#133,... 36 more fields] Batched: false, Format: Parquet, Location: InMemoryFileIndex[file:/home/cs143/data/submissions.parquet], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<archived:boolean,author:string,author_cakeday:boolean,author_flair_css_class:string,author...

Spark seems to be using a SortMergeJoin, which involves sorting each table by the join key, "link_id", in ascending order with the null values first. Then it partitions the key-value pairs
with a hash function on the keys to divide the data up. Then it projects the columns for each table from the corresponding parquet files. Finally it executes the merge join.

"""
