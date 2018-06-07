# If you have the models, df8 saved

#!/usr/bin/env python

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
model = CountVectorizerModel.load('countvec.model')
posModel = CrossValidatorModel.load('pos.model')
negModel = CrossValidatorModel.load('neg.model')

sc.addPyFile("cleantext.py")
from cleantext import sanitize

def doStuff(grams):
    res = []
    for gram in grams:
        for str in gram.split():
            if str is not None:
                res.append(str)
    return res

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
f = udf(lambda x: doStuff(sanitize(x)[1:]), ArrayType(StringType()));

df8 = sqlContext.read.parquet('df9.parquet')

sample = res.sample(False, 0.1, None)
sample.write.parquet('sample-final.parquet')
