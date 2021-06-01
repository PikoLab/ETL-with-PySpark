# ETL with PySpark

### 1. Purpose 

* Tracking covid-19 `Vaccinations` across countries
* Tracking  covid-19 `inflection rate` and `death rate`  across countries

### 2. ETL Process

```
Step1: Extract Data From HDFS

Step2: Transform data from csv file to spark dataframe 

Step3: (LAB1) Load data to MySQL database by jdbc connector
Step3: (LAB2) Load data to PostgreSQL database by docker-compose 
```

### 3. PySpark Useful SQL API 
1. select()
2. filter() / where()
3. isNotNull()
4. format_number(col, digit)
5. groupBy()
6. agg()
7. withColumn()
8. withColumnRenamed()
9. orderBy() / sort()
10. fillna() / na.fill()
11. join()
12. show()
13. window function: sum(col).over(partitionBy(col).orderBy(col))
14. write
15. jdbc(url, table, mode=None,properties=None)
16. limit()
17. count()
18. distinct()
19. drop()
20. dropna()
21. describe()
