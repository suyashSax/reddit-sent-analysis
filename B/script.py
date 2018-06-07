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
df8 = df8.where(df8["body"][0:3] != "&gt")
df8 = df8.where(df8["body"].contains("/s") == False)

# Repeat task 4, 5 and 6A
df9 = df8.select('*', f('body').alias('grams'))
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

sample = res.sample(False, 0.2, None)
sample.write.parquet('sample-final.parquet')
