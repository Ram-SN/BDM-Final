
import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext


def centerlineMatch(b, year, county, house_num1, house_num2, street): 
    
    for val in b.value:
        phys_id = val[0]
        L_LOW_HN = val[1]
        L_HIGH_HN = val[2]
        R_LOW_HN = val[3]
        R_HIGH_HN = val[4]
        L_LOW_HN_1 = val[8]
        L_LOW_HN_2 = val[9]
        L_HIGH_HN_1 = val[10]
        L_HIGH_HN_2 = val[11]
        R_LOW_HN_1 = val[12]
        R_LOW_HN_2 = val[13]
        R_HIGH_HN_1 = val[14]
        R_HIGH_HN_2 = val[15]
        BOROCODE = val[5]
        ST_LABEL = val[6]
        FULL_STREE = val[7]
                
        if house_num2:
            if R_LOW_HN_2 and R_HIGH_HN_2 and R_LOW_HN_1 and R_HIGH_HN_1:
                if(house_num2%2==0): 
                
                    if((house_num2 >= R_LOW_HN_2) and (house_num2 <= R_HIGH_HN_2) and (house_num1 >= R_LOW_HN_1) and (house_num1 <= R_HIGH_HN_1)
                      and (BOROCODE == county) and ((ST_LABEL == street) | (FULL_STREE == street))):
                        return(phys_id, year)
            
                else:
                    if((house_num2 >= L_LOW_HN_2) and (house_num2 <= L_HIGH_HN_2) and (house_num1 >= L_LOW_HN_1) and (house_num1 <= R_HIGH_HN_1)
                      and (BOROCODE == county) and ((ST_LABEL == street) | (FULL_STREE == street))):
                        return(phys_id, year)
        else:
            if R_LOW_HN and R_HIGH_HN:
                if(house_num1%2==0):
                    if((house_num1 >= R_LOW_HN) and (house_num1 <= R_HIGH_HN) and (BOROCODE == county) and ((ST_LABEL == street) | (FULL_STREE == street))):
                        return(phys_id, year)
                else:
                    if((house_num1 >= L_LOW_HN) and (house_num1 <= L_HIGH_HN) and (BOROCODE == county) and ((ST_LABEL == street) | (FULL_STREE == street))):
                        return(phys_id, year)
    return(None, year)

def processTrips(pid, records):

    import csv
    from datetime import datetime
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            
            year = row[4]
            county = row[21]
            house = row[23]
            street = row[24]
                        
            if (county == 'K' or county == 'BK' or county == 'KING' or county == 'KINGS'):
                county = 3
            elif (county == 'QUEEN' or county == 'Q' or county == 'QN' or county == 'QNS' or county == 'QU'):
                county = 4
            elif (county == 'MN' or county == 'NY' or county == 'MAN' or county == 'MH' or county == 'NEWY' or county == 'NEW Y'):
                county = 1
            elif (county == 'BX' or county == 'BRONX'):
                county = 2
            elif (county == 'ST' or county == 'R' or county == 'RICHMOND'):
                county = 5
            
            date_object = datetime.strptime(year, '%m/%d/%Y')
            
            if date_object.year:
                if county:
                    if house:
                        if street:
                            ## TODO Pass all these values to a function which matches on these and returns physical ID and year
                            if "-" in house:
                                house_num1,house_num2 = abc.split('-')
                            else:
                                house_num1 = house
                                house_num2 = None
                            
                            house_num1 = int(house_num1)
                            if house_num2:
                                house_num2 = int(house_num2)
                            county = int(county)
                            
                            street = street.upper()

                            phys_id,return_year = centerlineMatch(b, date_object.year, county, house_num1, house_num2, street)

                            if phys_id:
                                counts[phys_id,return_year] = counts.get((phys_id,return_year), 0) + 1

        
        except(ValueError, IndexError):
            pass
            
    return counts.items()
            

if __name__=='__main__':

    sc = SparkContext.getOrCreate()
    from pyspark.sql.functions import *

    spark = SparkSession(sc)

    centerline = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv', header=True, escape ='"', inferSchema = True, multiLine = True).cache()

    centerline.createOrReplaceTempView('centerline')

    columns = ['L_LOW_HN','L_HIGH_HN','R_LOW_HN','R_HIGH_HN',
             'L_LOW_HN_1','L_LOW_HN_2','L_HIGH_HN_1','L_HIGH_HN_2',
             'R_LOW_HN_1','R_LOW_HN_2','R_HIGH_HN_1','R_HIGH_HN_2']

    street_columns = ['FULL_STREE', 'ST_LABEL']

    split_col = pyspark.sql.functions.split(centerline['L_LOW_HN'], '-')
    centerline = centerline.withColumn('L_LOW_HN_1', split_col.getItem(0))
    centerline = centerline.withColumn('L_LOW_HN_2', split_col.getItem(1))

    split_col = pyspark.sql.functions.split(centerline['L_HIGH_HN'], '-')
    centerline = centerline.withColumn('L_HIGH_HN_1', split_col.getItem(0))
    centerline = centerline.withColumn('L_HIGH_HN_2', split_col.getItem(1))

    split_col = pyspark.sql.functions.split(centerline['R_LOW_HN'], '-')
    centerline = centerline.withColumn('R_LOW_HN_1', split_col.getItem(0))
    centerline = centerline.withColumn('R_LOW_HN_2', split_col.getItem(1))

    split_col = pyspark.sql.functions.split(centerline['R_HIGH_HN'], '-')
    centerline = centerline.withColumn('R_HIGH_HN_1', split_col.getItem(0))
    centerline = centerline.withColumn('R_HIGH_HN_2', split_col.getItem(1))

    for c in columns:
        centerline = centerline.withColumn(c, centerline[c].cast('int'))

    for s in street_columns:
        centerline = centerline.withColumn(s, centerline[s].cast('string'))
    
    centerline = centerline.select('PHYSICALID','L_LOW_HN','L_HIGH_HN','R_LOW_HN','R_HIGH_HN','BOROCODE',
                              'ST_LABEL','FULL_STREE','L_LOW_HN_1','L_LOW_HN_2','L_HIGH_HN_1','L_HIGH_HN_2','R_LOW_HN_1','R_LOW_HN_2','R_HIGH_HN_1','R_HIGH_HN_2')

    
    b = sc.broadcast(centerline.collect())

    rdd = sc.textFile('hdfs:///tmp/bdm/nyc_parking_violation/')
    counts = rdd.mapPartitionsWithIndex(processTrips)\
            .sortBy(lambda x: x[0], ascending=True)\
            .reduceByKey(lambda x,y: x+y) \
            .collect()
