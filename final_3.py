from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, when
from functools import reduce  
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import statsmodels.api as sm
import time


def clean_violations(violations):
    
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
    violations = spark.sql('SELECT * FROM violations WHERE Year >= 2011 AND Year <= 2015')
    violations = violations.groupby('House Number','Street Name','Violation County','Year','House_Num1','House_Num2').count()
    print("Done performing preprocessing for Violations, now moving to Centerline")
    
    return(violations)
    
def clean_centerline(centerline):
    
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
    
    return(centerline)

def joins(violations, centerlne):

    cond1 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 0,
         violations['House_Num1'] >= centerline['R_LOW_HN_1'],
         violations['House_Num1'] <= centerline['R_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond1_violations = violations.join(centerline.hint("broadcast"), cond1, 'right')

    cond2 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 1,
         violations['House_Num1'] >= centerline['L_LOW_HN_1'],
         violations['House_Num1'] <= centerline['L_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond2_violations = violations.join(centerline.hint("broadcast"), cond2, 'right')

    cond3 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 0,
         violations['House_Num2'] >= centerline['R_LOW_HN_2'],
         violations['House_Num2'] <= centerline['R_HIGH_HN_2'],
         violations['House_Num1'] >= centerline['R_LOW_HN_1'],
         violations['House_Num1'] <= centerline['R_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond3_violations = violations.join(centerline.hint("broadcast"), cond3, 'right')

    cond4 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 1,
         violations['House_Num2'] >= centerline['L_LOW_HN_2'],
         violations['House_Num2'] <= centerline['L_HIGH_HN_2'],
         violations['House_Num1'] >= centerline['L_LOW_HN_1'],
         violations['House_Num1'] <= centerline['L_HIGH_HN_1'],
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']) | (violations['Street Name'] == centerline['ST_LABEL']))]
    cond4_violations = violations.join(centerline.hint("broadcast"), cond4, 'right')

    print("conditional joins created, moving to the union")

    return(cond1_violations,cond2_violations,cond3_violations,cond4_violations)

def unionAll(*dfs):
    
    print("Union done, we now have the final data set to use")

    return reduce(DataFrame.unionAll, dfs)

def pivot_result(result):
    
    result.createOrReplaceTempView('result')
    x_pivot = result.groupBy('PHYSICALID')\
        .pivot("Year",["2011","2012","2013","2014","2015"])\
        .count()\
        .orderBy(['PHYSICALID'], ascending=True)

    x_pivot = x_pivot.na.fill(0)

    print('Pivot table with years have been created, now creating the ols column for all rows')

    return(x_pivot)

def my_ols(a,b,c,d,e):
    
    y = ([a,b,c,d,e])
    x = ([2015,2016,2017,2018,2019])
    x = sm.add_constant(x)
    model = sm.OLS(y,x)
    results = model.fit()
    
    print("Final Output")

    return((results.params[1]))


if __name__=='__main__':
    sc = SparkContext().getOrCreate()
    spark = SparkSession(sc)
    start = time.time()
    
    violations = spark.read.csv('hdfs:///tmp/bdm/nyc_parking_violation/2015.csv', 
                    header = True,
                    escape ='"',
                    inferSchema = True,
                    multiLine=True).cache()
    violations.createOrReplaceTempView('violations')


    centerline = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv', 
                    header = True,
                    escape ='"',
                    inferSchema = True,
                    multiLine=True).cache()
    centerline.createOrReplaceTempView('centerline')
    
    violations = clean_violations(violations)
#     violations.show()
    
    centerline = clean_centerline(centerline)
#     centerline.show()
    
    cond1_violations,cond2_violations,cond3_violations,cond4_violations = joins(violations, centerline)
    
    cond1_violations.createOrReplaceTempView('cond1_violations')
    cond2_violations.createOrReplaceTempView('cond2_violations')
    cond3_violations.createOrReplaceTempView('cond3_violations')
    cond4_violations.createOrReplaceTempView('cond4_violations')
    result = unionAll(cond1_violations, cond2_violations, cond3_violations, cond4_violations)
#     result.show()
    
    result.createOrReplaceTempView('result')
    x_pivot = pivot_result(result)
#     x_pivot.show()
    
    x_pivot.createOrReplaceTempView('x_pivot')
    
    x_pivot = x_pivot.withColumn("OLS_COEFF", my_ols(x_pivot['2011'],x_pivot['2012'],x_pivot['2013'],x_pivot['2014'],x_pivot['2015']))

    x_pivot = x_pivot.withColumn("OLS_COEFF", F.round("OLS_COEFF",2))
    
    end = time.time()
    x_pivot.show()
    print(end-start)