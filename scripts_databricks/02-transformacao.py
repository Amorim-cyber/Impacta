from pyspark.sql.functions import to_date, first, col, round, concat, lit

df_posts = spark.read.parquet("dbfs:/databricks-results/ludo/bronze/*/*")

day = df_posts['post_time'].substr(1,2)
month = df_posts['post_time'].substr(4,5).substr(1,2)
year = concat(lit(20),df_posts['post_time'].substr(7,8).substr(1,2).cast('string'))
year2 = df_posts['post_time'].cast('string').substr(7,8).substr(1,4)

df_posts = df_posts.withColumn('data', when(col('url') != "", to_date(concat(year,lit("-"),month,lit("-"),day))) \
    .otherwise(to_date(concat(year2,lit("-"),month,lit("-"),day)))) 
df_posts = df_posts.withColumn('begin_date', when(col('url') != "", to_date(concat(year,lit("-"),month,lit("-01")))) \
    .otherwise(to_date(concat(year2,lit("-"),month,lit("-01"))))) 

df_topic_group = df_posts.select("begin_date","topic_group")
df_topic_name = df_posts.select("topic_name")
df_user_name = df_posts.select("user_name")
df_posts = df_posts.select("data","topic_group","topic_name","user_name","url")


df_topic_group = df_topic_group.groupBy('begin_date','topic_group').count()

resultado_topic_group = df_topic_group.groupBy("begin_date") \
           .pivot("topic_group") \
           .agg(first("count")) \
           .orderBy("begin_date", ascending=False)

columns =  resultado_topic_group.schema.fields

for column in columns:
    if column.name != 'begin_date':
        resultado_topic_group = resultado_topic_group.na.fill(value=0,subset=[column.name])

resultado_topic_name = df_topic_name.groupBy('topic_name').count().orderBy('count',ascending=False).limit(5).coalesce(1)
resultado_user_name = df_user_name.groupBy('user_name').count().orderBy('count',ascending=False).limit(5).coalesce(1)
resultado_topic_group = resultado_topic_group.coalesce(1)
resultado_posts = df_posts.coalesce(1)

dbutils.fs.rm("dbfs:/databricks-results/ludo/prata", True)

resultado_posts.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/ludo/prata/posts")

resultado_topic_group.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/ludo/prata/topic_group")

resultado_user_name.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/ludo/prata/user_name")

resultado_topic_name.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/ludo/prata/topic_name")