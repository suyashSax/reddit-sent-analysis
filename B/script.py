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
from pyspark.ml.feature import CountVectorizer

cv = CountVectorizer(inputCol="grams", outputCol="feature", minDF=5.0, binary=True, vocabSize=1<<18)
model = cv.fit(data)
result = model.transform(data)
result.show(truncate=False)

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
    if(label == 1): return 1
    else:
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
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

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

posModel.save("pos.model")
negModel.save("neg.model")
