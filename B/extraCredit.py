# tests extra credit analysis of GOP and DEM data

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
part_a.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_a.csv")


part_a_dem = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative from task10_dem')
part_a_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_a_dem.csv")

part_a_gop = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative from task10_gop')
part_a_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_a_gop.csv")


part_b= sqlContext.sql('select avg(pos) as Positive, avg(neg) as Negative, DATE(FROM_UNIXTIME(created_utc)) as date from task10 group by date ORDER BY date')
part_b.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_b.csv")


part_b_dem= sqlContext.sql('select avg(pos) as Positive, avg(neg) as Negative, DATE(FROM_UNIXTIME(created_utc)) as date from task10_dem group by date ORDER BY date')
part_b_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_b_dem.csv")


part_b_gop= sqlContext.sql('select avg(pos) as Positive, avg(neg) as Negative, DATE(FROM_UNIXTIME(created_utc)) as date from task10_gop group by date ORDER BY date')
part_b_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_b_gop.csv")


part_c = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) as Positive, 100*avg(neg) as Negative, 100*avg(pos) -100*avg(neg) as Difference from task10 where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\' ,\'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\',\'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_c.csv")



part_c_dem = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) as Positive, 100*avg(neg) as Negative, 100*avg(pos) - 100*avg(neg) as Difference from task10_dem where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\',\'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_c_dem.csv")


part_c_gop = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) as Positive, 100*avg(neg) as Negative, 100*avg(pos) - 100*avg(neg) as Difference from task10_gop where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
part_c_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_c_gop.csv")



part_d_by_comment_score_dem = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, comment_score from task10_dem GROUP BY comment_score')
part_d_by_comment_score_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_comment_score_dem.csv")
part_d_by_comment_score_gop = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, comment_score from task10_gop GROUP BY comment_score')
part_d_by_comment_score_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_comment_score_gop.csv")
part_d_by_comment_score = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, comment_score from task10GROUP BY comment_score')
part_d_by_comment_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_comment_score.csv")



part_d_by_story_score = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, story_score from task10 GROUP BY story_score')
part_d_by_story_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_story_score.csv")
part_d_by_story_score_dem = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, story_score from task10_dem GROUP BY story_score')
part_d_by_story_score_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_story_score_dem.csv")
part_d_by_story_score_gop = sqlContext.sql('select 100*avg(pos) as Positive, 100*avg(neg) as Negative, story_score from task10_gop GROUP BY story_score')
part_d_by_story_score_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task10_part_d_story_score_gop.csv")


state_posneg_diff = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) - 100*avg(neg) as Difference from task10 where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
state_posneg_diff.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("difference.csv")
state_posneg_diff_dem = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) - 100*avg(neg) as Difference from task10_dem where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
state_posneg_diff_dem.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("difference_dem.csv")

state_posneg_diff_gop = sqlContext.sql('select author_flair_text AS state,  100*avg(pos) - 100*avg(neg) as Difference from task10_gop where author_flair_text IN (\'Alabama\', \'Alaska\', \'Arizona\', \'Arkansas\', \'California\', \'Colorado\', \'Connecticut\', \'Delaware\', \'District of Columbia\', \'Florida\', \'Georgia\', \'Hawaii\', \'Idaho\', \'Illinois\', \'Indiana\', \'Iowa\', \'Kansas\', \'Kentucky\', \'Louisiana\', \'Maine\', \'Maryland\', \'Massachusetts\', \'Michigan\', \'Minnesota\', \'Mississippi\', \'Missouri\', \'Montana\', \'Nebraska\', \'Nevada\', \'New Hampshire\', \'New Jersey\', \'New Mexico\', \'New York\', \'North Carolina\', \'North Dakota\', \'Ohio\', \'Oklahoma\', \'Oregon\', \'Pennsylvania\', \'Rhode Island\',\'South Carolina\', \'South Dakota\', \'Tennessee\', \'Texas\', \'Utah\', \'Vermont\', \'Virginia\', \'Washington\', \'West Virginia\', \'Wisconsin\', \'Wyoming\') group by author_flair_text ORDER BY author_flair_text')
state_posneg_diff_gop.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("difference_gop.csv")
