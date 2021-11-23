# Spark-Optimization-Mini-Project
Optimize the original query
##The original code took 1 s to run
```
%%time
#original
answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()```


##Switching the join order increased the time = 1.34 s
```
%%time

#switch join order

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = answers_month.join(questionsDF, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()
```

## caching the dataframe took least time =1.25
```
%%time
#cache option
answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

answers_month.cache()

resultDF = questionsDF.join(broadcast(answers_month), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()
```
## using repartition took least time =2.49s

```
%%time
#using repartition

repAnswersDF = answersDF \
    .repartition('question_id') \
    .withColumn('month', month('creation_date')) \
    .groupBy('question_id', 'month') \
    .agg(count('*').alias('cnt'))

repResultDF = questionsDF \
    .join(repAnswersDF, 'question_id') \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt') \
    .orderBy('question_id', 'month')
repResultDF.show()
```