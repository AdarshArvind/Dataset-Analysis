import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, Travis), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    
    # ADD YOUR CODE HERE
    with open(filename) as f:
    	number_of_trees = 0
    	for row in f:
    		number_of_trees=number_of_trees+1
    	return number_of_trees - 1
    
    raise ExceptionException("Not implemented yet")

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    with open(filename) as f:
    	tree_count = 0
    	data = csv.reader(f)
    	for row in data:
    		if len(row[6])>0:
    			tree_count=tree_count+1
    	return tree_count - 1
    
    raise Exception("Not implemented yet")

def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    # ADD YOUR CODE HERE
    with open(filename) as f:
    	unique_parks = []
    	string = ""
    	separator = '\n'
    	data = csv.reader(f)
    	for row in data:
    		if len(row[6])>0:
    			unique_parks.append(row[6])
    	unique_parks.sort()
    	A=set(unique_parks)
    	A=sorted(A)
    	A.remove('Nom_parc')
    	for element in A:
    		string = string+element+separator
    	return string
    
    raise Exception("Not implemented yet")

def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename) as f:
    	park_count = {}
    	count = 0
    	string = ""
    	separator = '\n'
    	data = csv.reader(f)
    	for row in data:
    		if len(row[6])>0:
    			if row[6] in park_count:
    				value = park_count[row[6]]
    				park_count[row[6]]=value+1
    			else:
    				park_count[row[6]]=1
    	del park_count["Nom_parc"]
    	# park_count = sorted(park_count.keys())
    	for st in sorted(park_count.items()):
    		string = string+str(st[0])+","+str(st[1])+separator
    	return string
    
    raise Exception("Not implemented yet")

def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename) as f:
    	park_count = {}
    	count = 0
    	string = ""
    	separator = '\n'
    	data = csv.reader(f)
    	for row in data:
    		if len(row[6])>0:
    			if row[6] in park_count:
    				value = park_count[row[6]]
    				park_count[row[6]]=value+1
    			else:
    				park_count[row[6]]=1
    	del park_count["Nom_parc"]
    	listoftuples = sorted(park_count.items(), key = lambda x:x[1])
    	# for elem in listoftuples:
    	# 	string += elem[0]+','+str(elem[1])+separator
    	for x in list(reversed(list(listoftuples))) [0:10]:
    		string += x[0]+','+str(x[1])+separator
    	return string
    
    raise Exception("Not implemented yet")
    
def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    with open(filename1, encoding='utf8') as f1:
    	unique_parks_f1 = []
    	data = csv.reader(f1)
    	for row in data:
    		if len(row[6])>0:
    			unique_parks_f1.append(row[6])
    	A=set(unique_parks_f1)
    	A.remove('Nom_parc')
    with open(filename2, encoding='utf8') as f2:
    	unique_parks_f2 = []
    	data = csv.reader(f2)
    	for row in data:
    		if len(row[6])>0:
    			unique_parks_f2.append(row[6])
    	B=set(unique_parks_f2)
    	B.remove('Nom_parc')
    	separator = '\n'
    output = []
    output = set(A) & set(B)
    output = sorted(output)
    str_output = ""
    for st in output:
    	str_output=str_output+st+separator
    # str_output = str_output.join(output)+separator
    return str_output

    raise Exception("Not implemented yet")

'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    lines = spark.sparkContext.textFile(filename)
    tree_count = lines.count()-1
    return tree_count

    raise Exception("Not implemented yet")

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    lines = spark.read.csv(filename, header=True).rdd
    output = lines.filter(lambda col: col['Nom_parc']!=None)
    result = output.count()
    return result

    raise Exception("Not implemented yet")

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    lines = spark.read.csv(filename, header=True).rdd
    output1 = lines.filter(lambda col: col['Nom_parc']!=None)
    output = output1.map(lambda col: col['Nom_parc'])
    unique = output.distinct()
    park = ""
    separator = '\n'
    unique = unique.sortBy(lambda col: col).collect()
    for element in unique:
    	park = park+element+separator
    return park

    raise Exception("Not implemented yet")

def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    lines = spark.read.csv(filename, header=True).rdd
    output1 = lines.filter(lambda col: col['Nom_parc']!=None)
    output = output1.map(lambda col: (col['Nom_parc'], 1)).reduceByKey(lambda col, y:col+y).sortByKey().collect()
    park = ""
    separator = '\n'
    for i in output:
    	park += i[0]+','+str(i[1])+separator
    # dictionary = {}
    # dictionary = unique.collectAsMap()
    # for element in dictionary:
    # 	park = park+element+separator
    return park

    raise Exception("Not implemented yet")

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    lines = spark.read.csv(filename, header=True).rdd
    output1 = lines.filter(lambda col: col['Nom_parc']!=None)
    output = output1.map(lambda col: (col['Nom_parc'], 1)).reduceByKey(lambda col, y:col+y).sortBy(lambda col: -col[1])
    park = ""
    separator = '\n'
    frequent = output.take(10)
    for i in frequent:
    	park += i[0]+','+str(i[1])+separator
    return park

    raise Exception("Not implemented yet")

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    line1 = spark.read.csv(filename1, header=True).rdd
    line2 = spark.read.csv(filename2, header=True).rdd
    output1 = line1.filter(lambda col: col['Nom_parc']!=None).map(lambda col: col['Nom_parc']).distinct()
    output2 = line2.filter(lambda col: col['Nom_parc']!=None).map(lambda col: col['Nom_parc']).distinct()
    output1 = output1.intersection(output2).collect()
    output = sorted(output1)
    # parks = toCSVLine(output1)
    parks = ""
    separator = '\n'
    for i in output:
    	parks += str(i)+separator
    return parks

    raise Exception("Not implemented yet")


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark.read.csv(filename, header=True)
    tree_count = df.count()
    return tree_count

    raise Exception("Not implemented yet")

def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark.read.csv(filename, header=True)
    df = df.select('Nom_parc').filter(df.Nom_parc != '')
    return df.count()

    raise Exception("Not implemented yet")

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark.read.csv(filename, header=True)
    df = df.select('Nom_parc').filter(df.Nom_parc != '').distinct()
    df = df.sort('Nom_parc',ascending=True)
    output = ""
    output = toCSVLine(df)
    return output

    raise Exception("Not implemented yet")

def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark.read.csv(filename, header=True)
    df = df.select('Nom_parc').filter(df.Nom_parc != '')
    df = df.sort('Nom_parc',ascending=True)
    df = df.groupBy('Nom_parc').count()
    output = ""
    output = toCSVLine(df)
    return output

    raise Exception("Not implemented yet")

def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df = spark.read.csv(filename, header=True)
    df = df.select('Nom_parc').filter(df.Nom_parc != '')
    df = df.sort('Nom_parc',ascending=True)
    df = df.groupBy('Nom_parc').count()
    df = df.sort('count', ascending=False)
    df = df.limit(10)
    output = ""
    output = toCSVLine(df)
    return output

    raise Exception("Not implemented yet")

def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    df1 = spark.read.csv(filename1, header=True)
    df1 = df1.select('Nom_parc').filter(df1.Nom_parc != '')

    df2 = spark.read.csv(filename2, header=True)
    df2 = df2.select('Nom_parc').filter(df2.Nom_parc != '')

    #df = df1.join(df2, df2.Nom_parc == df1.Nom_parc)
    df = df1.select('Nom_parc').intersect(df2.select('Nom_parc')).sort('Nom_parc', ascending=True)
    output = ""
    output = toCSVLine(df)
    return output
    raise Exception("Not implemented yet")

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    dd = df.read_csv(filename, dtype={'Nom_parc':str})
    tree_count = len(dd)
    return tree_count

    raise Exception("Not implemented yet")

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    dd = df.read_csv(filename, dtype={'Nom_parc': str})
    dd_filtered = len(dd[dd.Nom_parc.notnull()])
    return dd_filtered

    raise Exception("Not implemented yet")

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    # ADD YOUR CODE HERE
#     dd = df.read_csv(filename, dtype={'Nom_parc': str})
#     dd_filtered = dd[dd.Nom_parc.notnull()]
#     dictionary = {}
#     val = 0
#     string = ""
#     separator = '\n'
#     for element in dd_filtered.Nom_parc.iteritems():
#     	if element[1] in dictionary:
#     		val = dictionary[element[1]]
#     		dictionary[element[1]]=val+1
#     	else:
#     		dictionary[element[1]]=1
#     for ele in sorted(dictionary.keys()):
#     	string = string+ele+separator
#     return string
    
#     dd = df.read_csv(filename, dtype={'Nom_parc': str})
#     dd_filtered = dd[dd.Nom_parc.notnull()]
#     dd_filtered = dd_filtered.drop_duplicates().compute()
#     res = dd_filtered.sort_values(by='Nom_parc')['Nom_parc']
#     string = ""
#     separator = '\n'
#     for i in res:
#     	string += i+separator
# #     file1 = open("unique_parks_dask3.txt", "w+")
# #     file1.write(string)
# #     file1.close()
#     return string

    dd = df.read_csv(filename, dtype={'Nom_parc': str})
    dd_filtered = dd.Nom_parc.dropna()
    dd_filtered = dd_filtered.drop_duplicates().compute()
    res = dd_filtered.sort_values()
    string = ""
    separator = '\n'
    for i in res:
    	string += i+separator
#     file1 = open("unique_parks_dask3.txt", "w+")
#     file1.write(string)
#     file1.close()
    return string

#     dd = df.read_csv(filename, dtype={'Nom_parc': str})
#     dd = dd.Nom_parc.dropna().to_frame()
#     dd1 = dd.Nom_parc.to_bag()
#     uni = dict(dd1.frequencies(sort=True))
#     out = ""
#     for x in uni.keys():
#     	out = out+x+'\n'
#     # print(uni)
#     return out

    raise Exception("Not implemented yet")

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    dd = df.read_csv(filename, dtype={'Nom_parc': str})
    dd_filtered = dd[dd.Nom_parc.notnull()]
    db = dd_filtered.to_bag()
    result = (db.filter(lambda col: col[6]!=None)
    		.map(lambda col: col[6])
    		.frequencies())
    
    # dd_filtered = dd_filtered.drop_duplicates().compute()
    # out = dd_filtered.groupby(by = 'Nom_parc')['Nom_parc'].value_counts().compute().sort_values(ascending=False)
    string = ""
    separator = '\n'
    for i in sorted(result):
    	string+=i[0]+','+str(i[1])+'\n'
    
    return string
    raise Exception("Not implemented yet")

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
#     dd = df.read_csv(filename, dtype={'Nom_parc': str})
#     dd_filtered = dd[dd.Nom_parc.notnull()]
#     db = dd_filtered.to_bag()
#     result = (db.filter(lambda col: col[6]!=None)
#     		.map(lambda col: col[6])
#     		.frequencies(sort=True).topk(10))
#     string = ""
#     separator = '\n'
#     for i in result:
#     	string+=i[0]+','+str(i[1])+'\n'
# #     file1 = open("unique_parks_count_dask5.txt", "w+")
# #     file1.write(string)
# #     file1.close()
#     return string

    dd = df.read_csv(filename, dtype={'Nom_parc': str})
    dd_filtered = dd[dd.Nom_parc.notnull()]
    db = dd_filtered.to_bag()
    result = (db.filter(lambda col: col[6]!=None)
    		.map(lambda col: col[6])
    		.frequencies().topk(10, key=1))
    # frame = result.to_dataframe()
    # frame = frame.sort_values(by = 'Nom_parc')
    listoftuples = sorted(result, key = lambda col: -col[1])
    string = ""
    separator = '\n'
    for x in listoftuples:
    		string += x[0]+','+str(x[1])+separator
#     file1 = open("frequent_dask5.txt", "w+")
#     file1.write(string)
#     file1.close()
    return string

    raise Exception("Not implemented yet")

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    dd1 = df.read_csv(filename1, dtype={'Nom_parc': str})
    dd2 = df.read_csv(filename2, dtype={'No_Civiq': str,'Nom_parc': str})
    # dd_join = dd1.merge(dd2).drop_duplicates()
    dd_join1 = dd1.dropna(how = 'all', subset=['Nom_parc'])[['Nom_parc']]
    dd_join1 = dd_join1.drop_duplicates().compute()
    dd_join2 = dd2.dropna(how = 'all', subset=['Nom_parc'])[['Nom_parc']]
    dd_join2 = dd_join2.drop_duplicates().compute()
    separator = '\n'
    output = separator.join(dd_join1.merge(dd_join2).sort_values(by='Nom_parc')['Nom_parc'])+separator
#     file1 = open("dask_ques6.txt", "w+")
#     file1.write(str(output))
#     file1.close()
    return output

    raise Exception("Not implemented yet")
