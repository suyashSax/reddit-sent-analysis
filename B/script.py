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

from cleantext import sanitize

def doStuff(grams):
    res = []
    for gram in grams:
        for str in gram.split():
            if str is not None:
                res.append(str)
    return res

f = udf(lambda x: doStuff(sanitize(x)[1:]), ArrayType(StringType()));
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

# TODO: process Gop and Dem columns for extra credit

# Add negative column to result
neg_func = udf(lambda x: negative_udf(x), IntegerType())
pos_func = udf(lambda x: positive_column(x), IntegerType())

negative = result.select('*', neg_func('trump').alias('negative'))
positive_negative = negative.select('*', pos_func('trump').alias('positive'))

# neg = positive_negative.where(positive_negative['negative']==1)
# pos = positive_negative.where(positive_negative['positive']==1)

neg = positive_negative.withColumnRenamed('negative', 'label')
pos = positive_negative.withColumnRenamed('positive', 'label')

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
    numFolds=5)
negCrossval = CrossValidator(
    estimator=neglr,
    evaluator=negEvaluator,
    estimatorParamMaps=negParamGrid,
    numFolds=5)

posTrain, posTest = pos.randomSplit([0.5, 0.5])
negTrain, negTest = neg.randomSplit([0.5, 0.5])

print("Training positive classifier...")
posModel = posCrossval.fit(posTrain)

print("Training negative classifier...")
negModel = negCrossval.fit(negTrain)

# posModel.save("pos.model")
# negModel.save("neg.model")

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
df8 = df8.where(df8["body"][0:3] != ">")
df8 = df8.where(df8["body"].contains("/s") == False)

# Repeat task 4, 5 and 6A
df9 = df8.select('*', f('body').alias('grams'))
r9 = model.transform(df9)

def posProb(x):
    if x[0] > 0.2:
        return 1
    else:
        return 0

def negProb(x):
    if x[0] > 0.25:
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

sample = res.sample(False, 0.2, None)
sample.write.parquet('sample-final.parquet')


#TASK 10

#trump
task10_df = sqlContext.read.parquet("final.parquet")

#dem
task10_df_dem = sqlContext.read.parquet("final_dem.parquet")

#gop
task10_df_gop = sqlContext.read.parquet("final_gop.parquet")

sqlContext.registerDataFrameAsTable(task10_df, "task10")
sqlContext.registerDataFrameAsTable(task10_df_dem, "task10_dem")
sqlContext.registerDataFrameAsTable(task10_df_gop, "task10_gop")

part_a = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative from task10')
part_a_dem = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative from task10_dem')
part_a_gop = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative from task10_gop')

part_a.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_a.csv")
part_a_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("dem_a.csv")
part_a_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("gop_a.csv")

#djt
part_b= sqlContext.sql('select avg(pos) as Positive, avg(neg) as Negative, DATE(FROM_UNIXTIME(created_utc)) as date from task10 group by date ORDER BY date')
part_b.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")

#dem
part_b_dem= sqlContext.sql('select avg(pos) as Positive, avg(neg) as Negative, DATE(FROM_UNIXTIME(created_utc)) as date from task10_dem group by date ORDER BY date')
part_b_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data_dem.csv")

#gop
part_b_gop= sqlContext.sql('select avg(pos) as Positive, avg(neg) as Negative, DATE(FROM_UNIXTIME(created_utc)) as date from task10_gop group by date ORDER BY date')
part_b.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data_gop.csv")


part_c = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) as Positive, 100*avg(neg) as Negative, 100*avg(pos) - 100*avg(neg) as Difference from task10 where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data.csv")

part_c_dem = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) as Positive, 100*avg(neg) as Negative, 100*avg(pos) - 100*avg(neg) as Difference from task10_dem where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data_dem.csv")

part_c_gop = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) as Positive, 100*avg(neg) as Negative, 100*avg(pos) - 100*avg(neg) as Difference from task10_gop where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data_gop.csv")



d_comment_score = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, comment_score from task10 GROUP BY comment_score')
d_comment_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_comment_score.csv")

d_comment_score_dem = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, comment_score from task10_dem GROUP BY comment_score')
d_comment_score_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score_dem.csv")

d_comment_score_gop = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, comment_score from task10_gop GROUP BY comment_score')
d_comment_score_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score_gop.csv")


d_story_score = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, story_score from task10 GROUP BY story_score')
d_story_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("story_score.csv")

d_story_score_dem = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, story_score from task10_dem GROUP BY story_score')
d_story_score_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("story_score_dem.csv")

d_story_score_gop = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, story_score from task10_gop GROUP BY story_score')
d_story_score_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("story_score_gop.csv")


posneg_difference = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) - 100*avg(neg) as Difference from task10 where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')

posneg_difference.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("difference.csv")

posneg_difference_dem = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) - 100*avg(neg) as Difference from task10_dem where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')

posneg_difference_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("difference_dem.csv")

posneg_difference_gop = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) - 100*avg(neg) as Difference from task10_gop where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')

posneg_difference_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("difference_gop.csv")


most_positive_stories = sqlContext.sql('select link_id as most_positive from task10 GROUP BY link_id ORDER BY avg(pos) DESC LIMIT 10')
most_negative_stories = sqlContext.sql('select link_id as most_negative from task10 GROUP BY link_id ORDER BY avg(neg) DESC LIMIT 10')
