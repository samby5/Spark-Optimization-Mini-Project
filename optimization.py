%%time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month
import os
spark = SparkSession.builder.appName('Optimize I').getOrCreate()
base_path = os.getcwd()
print(base_path)
project_path = r'C:\Users\samy8\Desktop\Work Lab\SpringBoard\github\Spark-Optimization-Mini-Project\Optimization\Optimization'#('/').join(base_path.split('/')[0:-3]) 
print (project_path)

answers_input_path = os.path.join(project_path, 'data/answers')
questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()
questionsDF = spark.read.option('path', questions_input_path).load()
print(answersDF.count())
print(questionsDF.count())

#original
answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()


%%time
#cache option
answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

answers_month.cache()

resultDF = questionsDF.join(broadcast(answers_month), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

%%time

#switch join order

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = answers_month.join(questionsDF, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

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