
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
	cursor = openconnection.cursor()
	
	table_query=[]
	#Select only those rangepartitions where the ratings' values can lie based on input range
	cursor.execute("select partitionnum from rangeratingsmetadata where (minrating <= " + str(ratingMinValue) + " and maxrating>= " + str(ratingMinValue) + ") or (minrating > " + str(ratingMinValue) + " and maxrating> " + str(ratingMinValue) + " and  minrating < " + str(ratingMaxValue) + " and maxrating< " + str(ratingMaxValue) + ") or (minrating < " + str(ratingMaxValue) + " and maxrating>= " + str(ratingMaxValue) + ")")
		
	row=cursor.fetchone()[0]
	#Select desired rows from each range partition
	while row is not None:
            table_query.append("select 'rangeratingspart" + str(row) + "' as partition_name, userid, movieid, rating from rangeratingspart" + str(row) +" where rating>=" + str(ratingMinValue) + " and rating<=" + str(ratingMaxValue) +"" )
	    row = cursor.fetchone()
	    if(row is not None):
			row=row[0]

	

	#Select the number of partitions for round-robin parrtitioning
	cursor.execute("select partitionnum from roundrobinratingsmetadata")
	rr_part=cursor.fetchone()[0]

	#Select desired records from each partition
	for i in range(rr_part):
		table_query.append("select 'roundrobinratingspart" + str(i) +"' as partition_name, userid, movieid, rating from roundrobinratingspart" + str(i) +" where rating>=" + str(ratingMinValue) + " and rating<=" + str(ratingMaxValue) +"" )
	
	#Combine the data
	query_union=" union all ".join(table_query)
	
	#Write results to a csv file on the path given as parameter
	output_query = "copy ({0}) to STDOUT delimiter as ','".format(query_union)
	with open(outputPath,'w') as f:
		cursor.copy_expert(output_query,f)



def PointQuery(ratingValue, openconnection, outputPath):
	cursor = openconnection.cursor()
	
	#Select only those range partitions where the given rating value can lie
	table_query=[]
	cursor.execute("select partitionnum from rangeratingsmetadata where " + str(ratingValue) + " >= minrating and " + str(ratingValue) + " <= maxrating")
		
	#Select desired records from the range partitions
	row=cursor.fetchone()[0]
	while row is not None:
            table_query.append("select 'rangeratingspart" + str(row) + "' as partition_name, userid, movieid, rating from rangeratingspart" + str(row) +" where rating =" + str(ratingValue) +"" )
	    row = cursor.fetchone()
	    if(row is not None):
			row=row[0]

	

	#Select number of partitions for round robin partitioning
	cursor.execute("select partitionnum from roundrobinratingsmetadata")
	rr_part=cursor.fetchone()[0]

	#Select desired records from round-robin partitions
	for i in range(rr_part):
		table_query.append("select 'roundrobinratingspart" + str(i) +"' as partition_name, userid, movieid, rating from roundrobinratingspart" + str(i) +" where rating =" + str(ratingValue) + "" )

	#Combine both the datasets
	query_union=" union all ".join(table_query)

	#Write results to a csv file on the path given as parameter
	output_query = "copy ({0}) to STDOUT delimiter as ','".format(query_union)
	with open(outputPath,'w') as f:
		cursor.copy_expert(output_query,f)
