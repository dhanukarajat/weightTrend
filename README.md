#weightTrend #BigData #Hadoop #MapReduce
Calculating Weight trend over 5 years for 7 different states along genders using Hadoop MapReduce

In this project we were asked to implement compute the weight trend over 5 years for 7 different states along genders using the given data. We needed to develop a map/reduce solution. For each of the following 7 states, we have calculated the average weight of males and females for each year (2009 - 2013) and plotted graphs with year as the X-axis and average weight as the Y-axis.

-> Implementation of Mappers and Reducers: 
The implementation of the mapper was very simple. However, the decision of key-value pairs was the deciding factor.
In this project, we have used following set of key-value pairs:
1. Mapper – Key should be <Year + Gender + State>. Value should be <Weight>
2. Reducer – Generates averages of Weight for each of the keys presented from the Mapper. 

-> We have created a Hash map for storing the 7 states and the gender values. Using this, we mapped the state numbers (integer) to the state names (String). 

-> Then we implemented the WeightMapper class to extract the given dataset on row basis. We created the above mentioned key-value pair from the given dataset and then stored the key in toReducer string which is passed on to the Reducer function. The critical part of this class was filtering the dataset for valid 7 states which has been asked to analyze.

-> In WeightReducer class, we calculated the average of all the weights by retrieving the weight values using get() function for each key pair, storing and adding it to totalWeight variable. Then we calculated the average of all the weights based on the counter and the total weight calculated above.

System Configuration: 
1. Hadoop version: 2.7.2 
2. Operating System : Ubuntu 16.04 (Running on a VMware Workstation 12) 
3. Java : Java 1.7.0