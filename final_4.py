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
import sys

# TODO values we get twice the intended (?) do we divide it by 2?
# TODO write comments in the code and add description to functions
# TODO change the variables names vastly
# TODO change the boroughs to what we used in the file "TEST-BDM.ipynb"

def clean_violations(violations):
    '''
    PREPROCESSING:
    This function takes in the violations (all five files) as input. 
    In this function, we clean the violations table and drop the unwanted columns.
    We split house number into two to accomadate for compound house numbers
    We return: the violations file after pivoting on the years to reduce number of rows
    '''

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

    violations_pivot = violations.groupby('Violation County','Street Name','House Number','House_Num1','House_Num2').pivot('Year',["2015","2016","2017","2018","2019"]).count().cache()
    
    print("Done performing preprocessing for Violations, now moving to Centerline")
    
    return(violations_pivot)

def clean_centerline(centerline):
    '''
    PREPROCESSING:
    This function takes in the centerline as the input.
    The role of this function is to clean the centerline file and split compound house numbers into two column (for each L and R combination)
    We replace rows that are same in FULL_STREE and ST_LABEL with '0' to avoid the OR operation during join
    We return: the centerline file after preprocessing
    '''
    
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

    centerline = centerline.withColumn('ST_LABEL', F.when(centerline['FULL_STREE'] == centerline['ST_LABEL'], '0').otherwise(centerline['ST_LABEL']))
    
    return(centerline)


def joins(violations, centerline):
    '''
    This function takes in the pivoted violations and cleaned centerline as input
    In this function, we perform a total of 8 joins. 4 joins for each condition as mentioned in the problem statement.
    Times 2 to check for the FULL_STREE and ST_LABEL. This was done to avoid the OR statement.
    We return dataframes created by joins on:

    (1) CONDITION 1 (FULL_STREE)
    (2) CONDITION 1 (ST_LABEL)
    (3) CONDITION 2 (FULL_STREE)
    (4) CONDITION 2 (ST_LABEL)
    (5) CONDITION 3 (FULL_STREE)
    (6) CONDITION 3 (ST_LABEL)
    (7) CONDITION 4 (FULL_STREE)
    (8) CONDITION 4 (ST_LABEL)
    '''

    cond1 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 0,
         ((violations['House_Num1'] >= centerline['R_LOW_HN_1']) & (violations['House_Num1'] <= centerline['R_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']))]
    cond1_violations = violations.join(centerline.hint("broadcast"), cond1, 'right').cache()

    cond1_1 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 0,
         ((violations['House_Num1'] >= centerline['R_LOW_HN_1']) & (violations['House_Num1'] <= centerline['R_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         (violations['Street Name'] == centerline['ST_LABEL'])]
    cond1_1_violations = violations.join(centerline.hint("broadcast"), cond1_1, 'right').cache()

    ####
    cond2 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 1,
         ((violations['House_Num1'] >= centerline['L_LOW_HN_1']) & (violations['House_Num1'] <= centerline['L_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']))]
    cond2_violations = violations.join(centerline.hint("broadcast"), cond2, 'right').cache()

    cond2_2 = [violations['House_Num2'].isNull(),
         violations['House_Num1'] % 2 == 1,
         ((violations['House_Num1'] >= centerline['L_LOW_HN_1']) & (violations['House_Num1'] <= centerline['L_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         (violations['Street Name'] == centerline['ST_LABEL'])]
    cond2_2_violations = violations.join(centerline.hint("broadcast"), cond2_2, 'right').cache()

    ####
    cond3 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 0,
         ((violations['House_Num2'] >= centerline['R_LOW_HN_2']) & (violations['House_Num2'] <= centerline['R_HIGH_HN_2'])),
         ((violations['House_Num1'] >= centerline['R_LOW_HN_1']) & (violations['House_Num1'] <= centerline['R_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']))]
    cond3_violations = violations.join(centerline.hint("broadcast"), cond3, 'right').cache()

    cond3_3 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 0,
         ((violations['House_Num2'] >= centerline['R_LOW_HN_2']) & (violations['House_Num2'] <= centerline['R_HIGH_HN_2'])),
         ((violations['House_Num1'] >= centerline['R_LOW_HN_1']) & (violations['House_Num1'] <= centerline['R_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         (violations['Street Name'] == centerline['ST_LABEL'])]
    cond3_3_violations = violations.join(centerline.hint("broadcast"), cond3_3, 'right').cache()

    ####
    cond4 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 1,
         ((violations['House_Num2'] >= centerline['L_LOW_HN_2']) & (violations['House_Num2'] <= centerline['L_HIGH_HN_2'])),
         ((violations['House_Num1'] >= centerline['L_LOW_HN_1']) & (violations['House_Num1'] <= centerline['L_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         ((violations['Street Name'] == centerline['FULL_STREE']))] 
    cond4_violations = violations.join(centerline.hint("broadcast"), cond4, 'right').cache()

    cond4_4 = [violations['House_Num2'].isNotNull(),
         violations['House_Num2'] % 2 == 1,
         ((violations['House_Num2'] >= centerline['L_LOW_HN_2']) & (violations['House_Num2'] <= centerline['L_HIGH_HN_2'])),
         ((violations['House_Num1'] >= centerline['L_LOW_HN_1']) & (violations['House_Num1'] <= centerline['L_HIGH_HN_1'])),
         violations['Violation County'] == centerline['BOROCODE'],
         (violations['Street Name'] == centerline['ST_LABEL'])]
    cond4_4_violations = violations.join(centerline.hint("broadcast"), cond4_4, 'right').cache()

    print("conditional joins created, moving to the union")

    return(cond1_violations,cond1_1_violations,cond2_violations,cond2_2_violations,cond3_violations,cond3_3_violations,cond4_violations,cond4_4_violations)


def unionAll(*dfs):
    '''
    This function takes in all the tables created from the "joins" function
    and returns the union all of the tables. Although UnionAll is deprecated in Spark 2.0.0+, it is still used.
    '''
    
    print("Union done, we now have the final data set to use")

    return reduce(DataFrame.unionAll, dfs)


def my_ols(a,b,c,d,e):
    '''
    This function takes in the five columns (2015,2016,2017,2018,2019). We use the columns from the pre_ols table.
    We return: the ols coefficient given every row.
    '''
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

    # Clearing the cache in hopes that it speeds up the process
    spark.catalog.clearCache()

    output_file = sys.argv[1] 

    # Getting the time from the start of the read file(s)
    start = time.time()
    
    violations = spark.read.csv('hdfs:///tmp/bdm/nyc_parking_violation/', 
                    header = True,
                    escape ='"',
                    inferSchema = True,
                    multiLine=True).cache()


    centerline = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv', 
                    header = True,
                    escape ='"',
                    inferSchema = True,
                    multiLine=True).cache()
    
    # Getting all the distinct physicalID to perform a union at the end to get all the physical IDs 
    centerline_distinct = centerline.select("PHYSICALID").distinct()\
                          .withColumn('2015',F.lit(0))\
                          .withColumn('2016',F.lit(0))\
                          .withColumn('2017',F.lit(0))\
                          .withColumn('2018',F.lit(0))\
                          .withColumn('2019',F.lit(0)).cache()
    
    violations_pivot = clean_violations(violations).cache()

    # The unpersist() function is used throughout this script in order to release dataframes from the cache
    violations.unpersist()
    
    centerline = clean_centerline(centerline)

    cond1_violations,cond1_1_violations,cond2_violations,cond2_2_violations,cond3_violations,cond3_3_violations,cond4_violations,cond4_4_violations = joins(violations_pivot, centerline)

    violations_pivot.unpersist()

    centerline.unpersist()

    result = unionAll(cond1_violations,cond1_1_violations,cond2_violations,cond2_2_violations,cond3_violations,cond3_3_violations,cond4_violations,cond4_4_violations).cache()

    cond1_violations.unpersist()
    cond2_violations.unpersist()
    cond3_violations.unpersist()
    cond4_violations.unpersist()

    # Here we fill in the values with 0 since these column contain NULL if there was no violations occurred
    result_2 = result.select('PHYSICALID','2015','2016','2017','2018','2019').na.fill(0).orderBy('PHYSICALID').cache()
   
    output_pre_ols = result_2.groupBy('PHYSICALID').sum().cache()

    result_2.unpersist()

    # Here we perform a union with the distinct centerline dataframe and pre_ols dataframe to fill in the missing Physical IDs
    # The resulting dataframe is grouped on "PHYSICALID" and aggregated to get the max of the values.
    output_pre_ols = output_pre_ols.union(centerline_distinct)\
                .groupBy("PHYSICALID")
                .agg(F.max('sum(2015)').alias('2015'),F.max('sum(2016)').alias('2016'), F.max('sum(2017)').alias('2017'), F.max('sum(2018)').alias('2018'), F.max('sum(2019)').alias('2019')).cache()

    # Here, we pass each of the year row as a value to the function to "my_ols", and create a new column "OLS_COEFF" with the output of it
    output_ols = output_pre_ols.withColumn("OLS_COEFF", my_ols(output_pre_ols['sum(2015)'],output_pre_ols['sum(2016)'],output_pre_ols['sum(2017)'],output_pre_ols['sum(2018)'],output_pre_ols['sum(2019)']))\
                               .withColumn("OLS_COEFF", F.round("OLS_COEFF", 3)).cache()


    # Selecting only the specific columns to make sure no unwanted column is present
    output_ols = output_ols.select('PHYSICALID','2015','2016','2017','2018','2019','OLS_COEFF')

    # output_ols.show()

    output_ols.write.csv(output_file, mode = 'overwrite')

    end = time.time()
    
    print(start-end)