#!/usr/bin/python2.7


import database_partitioning as database_partitioning
import query_processor as query_processor
if __name__ == '__main__':
    try:
        #Creating Database simplified_query_processor
        print "Creating Database named as simplified_query_processor"
        database_partitioning.createDB();

        # Getting connection to the database
        print "Getting connection from the simplified_query_processor database"
        con = database_partitioning.getOpenConnection();

        # Clear the database existing tables
        print "Delete tables"
        database_partitioning.deleteTables('all', con);

        # Loading Ratings table
        print "Creating and Loading the ratings table"
        database_partitioning.loadRatings('ratings', 'test_data.dat', con);

        # Doing Range Partition
        print "Doing the Range Partitions"
        database_partitioning.rangePartition('ratings', 5, con);

        # Doing Round Robin Partition
        print "Doing the Round Robin Partitions"
        database_partitioning.roundRobinPartition('ratings', 5, con);

        # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
        database_partitioning.deleteTables('ratings', con);

        # Calling RangeQuery
        print "Performing Range Query"
        query_processor.RangeQuery(1.5, 3.5, con, "./rangeResult.txt");
        #query_processor.RangeQuery(1,4,con, "./rangeResult.txt");

        # Calling PointQuery
        print "Performing Point Query"
        query_processor.PointQuery(4.5, con, "./pointResult.txt");
        #query_processor.PointQuery('2,con, "./pointResult.txt");
        
        # Deleting All Tables
        database_partitioning.deleteTables('all', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
