from pyspark.sql import SparkSession
from pyspark.sql import *
import pyspark.sql.functions as F


#-------------------------------------------------------------------------------

spark = SparkSession.builder\
    .config('spark.driver.extraClassPath','/home/user/shared_folder/postgresql-42.3.1.jar')\
    .master('local')\
    .appName('lesson_13')\
    .getOrCreate()

#-------------------------------------------------------------------------------

pg_url = "jdbc:postgresql://127.0.0.1:5432/postgres"
pg_creds={"user":"pguser","password":"secret"}


#-------------------------------------------------------------------------------

film_category = spark.read.jdbc(pg_url,table = 'film_category',
                    properties = pg_creds)
category = spark.read.jdbc(pg_url,table = 'category',
                    properties = pg_creds)
actor = spark.read.jdbc(pg_url,table = 'actor',
                    properties = pg_creds)
film_actor = spark.read.jdbc(pg_url,table = 'film_actor',
                    properties = pg_creds)
inventory = spark.read.jdbc(pg_url,table = 'inventory',
                    properties = pg_creds)
rental = spark.read.jdbc(pg_url,table = 'rental',
                    properties = pg_creds)
payment = spark.read.jdbc(pg_url,table = 'payment',
                    properties = pg_creds)
film = spark.read.jdbc(pg_url,table = 'film',
                    properties = pg_creds)
address= spark.read.jdbc(pg_url,table = 'address',
                    properties = pg_creds)
customer = spark.read.jdbc(pg_url,table = 'customer',
                    properties = pg_creds)
city= spark.read.jdbc(pg_url,table = 'city',
                    properties = pg_creds)

#-------------------------------------------------------------------------------
#1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
df1 = film_category.join(category
        ,film_category.category_id == category.category_id)\
    .groupBy('name')\
    .agg(F.count(film_category.category_id).alias('count'))\
    .sort(F.desc('count'))
df1.show(df1.count())

#-------------------------------------------------------------------------------
#2. вывести 10 актеров, чьи фильмы большего всего арендовали,
#отсортировать по убыванию.
df2 = actor.join(film_actor
            , 'actor_id')\
        .join(inventory
             ,'film_id' )\
        .join(rental
             ,inventory.inventory_id == rental.rental_id)\
        .groupBy('actor_id',F.concat('first_name',F.lit(' '),'last_name')
                .alias('Actor Name'))\
        .count()\
        .drop('actor_id')\
        .sort(F.desc('count'))\
        .limit(10)
df2.show()    

#-------------------------------------------------------------------------------
#3.вывести категорию фильмов, на которую потратили больше всего денег.
df3 = category\
        .join(film_category
             ,'category_id')\
        .join(inventory
             ,'film_id')\
        .join(rental
             ,'inventory_id')\
        .join(payment
             ,'rental_id')\
        .groupBy('name')\
        .sum('amount')\
        .sort(F.desc('sum(amount)'))\
        .drop('sum(amount)')\
        .limit(1)
df3.show()

#-------------------------------------------------------------------------------
#4.вывести названия фильмов, которых нет в inventory. 
df4 = film\
        .join(inventory
             ,'film_id','left')\
        .where(F.isnull(F.col('inventory_id')))\
        .select('title')\
        
df4.show(df4.count(), False)

#5.вывести топ 3 актеров, которые больше всего появлялись в фильмах 
#в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов,
#вывести всех..

#-------------------------------------------------------------------------------
df5 = actor\
        .join(film_actor
             ,'actor_id')\
        .join(film_category 
             ,'film_id')\
        .join(category.where(F.col('name')=='Children')
             ,'category_id')\
        .groupBy('actor_id',F.concat('first_name',F.lit(' '),'last_name').alias('Full name'))\
        .count()\
        .sort(F.desc('count'))

top3border = df5.take(3)[2]['count']

df5 = df5.where(F.col('count')>=top3border)\
        .drop('actor_id','count')

df5.show()

#-------------------------------------------------------------------------------
#6.вывести города с количеством активных и неактивных клиентов 
#(активный — customer.active = 1). 
#Отсортировать по количеству неактивных клиентов по убыванию.

df6 = city\
        .join(address ,'city_id','left')\
        .join(customer,'address_id','left')\
        .groupBy('city')\
        .agg(F.sum('active').alias('Active')
             ,(F.count('active')-F.sum('active')).alias('Non active'))\
        .na.fill(0)\
        .select('city','Active','Non active')\
        .sort(F.desc('Non active'))\

df6.show(df6.count())

#-------------------------------------------------------------------------------
#7.вывести категорию фильмов, у которой самое большое кол-во часов
#суммарной аренды в городах (customer.address_id в этом city),#
#и которые начинаются на букву “a”. То же самое сделать для городов
#в которых есть символ “-”.

def get_df7(like):
    df = category\
        .join(film_category  ,'category_id')\
        .join(inventory ,'film_id')\
        .join(rental  ,'inventory_id')\
        .join(customer  ,'customer_id')\
        .join(address  ,'address_id')\
        .join(city  ,'city_id')\
        .where(F.col('city').like(like))\
        .withColumn('return_date',F.col('return_date').cast('integer'))\
        .withColumn('rental_date',F.col('rental_date').cast('integer'))\
        .withColumn('len',(F.col('return_date')-F.col('rental_date')).cast('integer'))\
        .groupBy('name')\
        .sum('len')\
        .sort(F.desc('sum(len)'))\
        .limit(1)\
        .drop('sum(len)')
    return df
    
df7_1 = get_df7('%a')
df7_2 = get_df7('%-%')
df7 = df7_1.union(df7_2)
df7.show()