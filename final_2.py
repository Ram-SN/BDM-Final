from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, when
from functools import reduce  
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import LongType
from scipy.stats import linregress
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import IntegerType
import statsmodels.api as sm
import time

#### TODO does it become faster if i rewrite all this as functions?????
#### TODO rewrite the pivot block in spark sql
#### TODO change name of the violations file, test out with 1 file first
#### TODO change name of the centerline file
#### TODO remove the unnecessary headers
#### TODO add the timing component for the file whereever necessary
#### TODO get all the file names in the violations HDFS Folder
#### TODO Find ways to make code more efficient
#### TODO try ordering before the groupby
#### TODO find which linear regression model is faster to run
#### TODO if time permits, try everything in spark sql
#### TODO df.cache() for all dataframes
 

if __name__=='__main__':

	sc = SparkContext().getOrCreate()
    spark = SparkSession(sc)

    violations = spark.read.csv('pv_sample.csv', 
                    header = True,
                    escape ='"',
                    inferSchema = True,
                    multiLine=True).cache()
    violations.createOrReplaceTempView('violations')


    centerline = spark.read.csv('Centerline.csv', 
                    header = True,
                    escape ='"',
                    inferSchema = True,
                    multiLine=True).cache()
    centerline.createOrReplaceTempView('centerline')

    violations = violations.na.drop(subset=['Street Name','House Number','Violation County','Issue Date'])
    violations = violations.select('House Number','Street Name','Violation County', 'Issue Date')
    violations = violations.withColumn('Street Name', F.upper(F.col('Street Name')))
    violations = violations.withColumn("House Number", F.regexp_replace(F.col("House Number"), "[A-Z]", ""))
    split_year = F.split(violations['Issue Date'],'/')
    violations = violations.withColumn('Year',split_year.getItem(2)).drop('Issue Date')
    split_col = F.split(violations['House Number'],'-')
    violations = violations.withColumn('House_Num1',split_col.getItem(0).cast('int'))
    violations = violations.withColumn('House_Num2',split_col.getItem(1).cast('int'))

    boroughs = {'MAN':'1', 'MH':'1', 'MN':'1', 'NEWY':'1', 'NEW Y':'1', 'NY':'1',
           'BRONX':'2','BX':'2', 'PBX':'2',
           'BK':'3', 'K':'3', 'KING':'3', 'KINGS':'3',
           'Q':'4', 'QN':'4', 'QNS':'4', 'QU':'4','QUEEN':'4',
           'R':'5', 'RICHMOND':'5'}
    violations = violations.replace(boroughs, subset='Violation County')

    violations.createOrReplaceTempView('violations')
    violations = spark.sql('SELECT * FROM violations WHERE Year >= 2015 AND Year <= 2019')

    print("Done performing preprocessing for Violations, now moving to Centerline")

    ##########

    centerline = centerline.select('PHYSICALID','L_LOW_HN','L_HIGH_HN', 'R_LOW_HN','R_HIGH_HN','FULL_STREE','ST_LABEL','BOROCODE')
    centerline = centerline.na.drop(subset=['PHYSICALID','L_LOW_HN','L_HIGH_HN', 'R_LOW_HN','R_HIGH_HN','FULL_STREE','ST_LABEL','BOROCODE'])
    centerline = centerline.withColumn('FULL_STREE', F.upper(F.col('FULL_STREE'))).withColumn('ST_LABEL', F.upper(F.col('ST_LABEL')))

    split_col = F.split(centerline['L_LOW_HN'], '-')
    centerline = centerline.withColumn('L_LOW_HN_1', split_col.getItem(0).cast('int'))
    centerline = centerline.withColumn('L_LOW_HN_2', split_col.getItem(1).cast('int'))

    split_col = F.split(centerline['L_HIGH_HN'], '-')
    centerline = centerline.withColumn('L_HIGH_HN_1', split_col.getItem(0).cast('int'))
    centerline = centerline.withColumn('L_HIGH_HN_2', split_col.getItem(1).cast('int'))

    split_col = F.split(centerline['R_LOW_HN'], '-')
    centerline = centerline.withColumn('R_LOW_HN_1', split_col.getItem(0).cast('int'))
    centerline = centerline.withColumn('R_LOW_HN_2', split_col.getItem(1).cast('int'))

    split_col = F.split(centerline['R_HIGH_HN'], '-')
    centerline = centerline.withColumn('R_HIGH_HN_1', split_col.getItem(0).cast('int'))
    centerline = centerline.withColumn('R_HIGH_HN_2', split_col.getItem(1).cast('int'))

    print("Done performing preprocessing for Centerline, now moving to the conditional joins part")

    ##########

    cond1 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 0,
         violations['House_Num1'] >= centerline['R_LOW_HN_1'],
         violations['House_Num1'] <= centerline['R_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond1_violations = violations.join(centerline.hint("broadcast"), cond1)

    cond2 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 1,
         violations['House_Num1'] >= centerline['L_LOW_HN_1'],
         violations['House_Num1'] <= centerline['L_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond2_violations = violations.join(centerline.hint("broadcast"), cond2)

    cond3 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 0,
         violations['House_Num2'] >= centerline['R_LOW_HN_2'],
         violations['House_Num2'] <= centerline['R_HIGH_HN_2'],
         violations['House_Num1'] >= centerline['R_LOW_HN_1'],
         violations['House_Num1'] <= centerline['R_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond3_violations = violations.join(centerline.hint("broadcast"), cond3)

    cond4 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 1,
         violations['House_Num2'] >= centerline['L_LOW_HN_2'],
         violations['House_Num2'] <= centerline['L_HIGH_HN_2'],
         violations['House_Num1'] >= centerline['L_LOW_HN_1'],
         violations['House_Num1'] <= centerline['L_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond4_violations = violations.join(centerline.hint("broadcast"), cond4)

    print("conditional joins created, moving to the union")

    ########

    def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)

    result = unionAll(cond1_violations, cond2_violations, cond3_violations, cond4_violations)

    print("Union done, we now have the final data set to use")

    #######
    ####### RIGHT THIS BLOCK IN SPARK SQL!!!! #########
    ####### VERY INEFFICIENT #######

    result.createOrReplaceTempView('result')

    x_pivot = result.groupBy('PHYSICALID')\
        .pivot("Year",["2015","2016","2017","2018","2019"])\
        .count()\
        .orderBy(['PHYSICALID'], ascending=True)

    x_pivot = x_pivot.na.fill(0)

    print('Pivot table with years have been creates, now creating the ols column for all rows')

    ####### !!! ######
    #######

    def my_ols(a,b,c,d,e):
        y = ([a,b,c,d,e])
        x = ([2015,2016,2017,2018,2019])
        x = sm.add_constant(x)
        model = sm.OLS(y,x)
        results = model.fit()
        return((results.params[1]))

    x_pivot = x_pivot.withColumn("OLS_COEFF", my_ols(x_pivot['2015'],x_pivot['2016'],x_pivot['2017'],x_pivot['2018'],x_pivot['2019']))

    x_pivot = x_pivot.withColumn("OLS_COEFF", F.round("OLS_COEFF"))

    print("Done, time taken =")

    x_pivot.show(50)


