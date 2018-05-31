This is just the script.
You gotta add the datasets on ur own in the folder...


# To create parquet files:

```python
from pyspark.sql import SQLContext
# example to create parquet file to load faster
import itertools
from itertools import chain
from pyspark.sql.types import *
sqlContext = SQLContext(sc)
# read json into dataframe
df = spark.read.json("comments-minimal.json.bz2")
# write a parquet file
df.write.parquet("comments-minimal.parquet")
# read the parquet file, will be much faster than reading the json
data = sqlContext.read.parquet("comments-minimal.parquet")
# outputs contents of file
data.show()
```
