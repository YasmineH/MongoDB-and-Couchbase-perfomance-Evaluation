import pymongo
import csv
import json
import pprint
#import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
import time
from datetime import datetime
from couchbase.n1ql import N1QLQuery
from couchbase.bucket import Bucket


nb_operations = 100000
client = MongoClient()

#***********Read Data from csv
db = client.mydb    #create database
airports_collection = db.airports   #create collection
flights_collection = db.flights   #create collection
flights_25_collection = db.flights_25   #create collection
flights_50_collection = db.flights_50   #create collection
flights_75_collection = db.flights_75   #create collection

# airports_collection.createIndex({"id": 1})
# flights_collection.createIndex()
# flights_25_collection.createIndex()
# flights_50_collection.createIndex()
# flights_75_collection.createIndex()

#******* read data ********
def read_airport():
	t1=time.time()
	for i in (0, nb_operations-1):
		airports_collection.find({"IATA/FAA":"NY"})
		i+=1
	t2 = time.time()
	print("-Airports- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Airports- Reading Throughput time : ", nb_operations/(t2-t1))

#******* write data ********
def write_airport():
	t1=time.time()
	for i in (0, nb_operations-1):
		airports_collection.insert({
							    "City": "Alger",
							    "Name": "Alger",
							    "Country": "ALG",
							    "IATA/FAA": "LG",
							    "Longitude": 33.2015961,
							    "Latitude": 35.71765889,
							    "id": "23632"
							  })
		i+=1
	t2 = time.time()
	print("-Airports- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Airports- Writing Throughput time : ", nb_operations/(t2-t1))

#******* order BY ********
def orderby_airport():
	t1=time.time()
	for i in (0, nb_operations-1):
		airports_collection.Account.find().sort("City")
		i+=1
	t2 = time.time()
	print("-Airports- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Airports- orderby Throughput time : ", nb_operations/(t2-t1))

#******* read data ********
def read_flights():
	t1=time.time()
	for i in (0, nb_operations-1):
		flights_collection.find({"Dest":"JFK"})
		i+=1
	t2 = time.time()
	print("-Flights_25- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- Reading Throughput time : ", nb_operations/(t2-t1))
	
	t1=time.time()
	for i in (0, nb_operations-1):
		flights_25_collection.find({"Dest":"JFK"})
		i+=1
	t2 = time.time()
	print("-Flights_50- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- Reading Throughput time : ", nb_operations/(t2-t1))
	t1=time.time()
	for i in (0, nb_operations-1):
		flights_50_collection.find({"Dest":"JFK"})
		i+=1
	t2 = time.time()
	print("-Flights_75- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- Reading Throughput time : ", nb_operations/(t2-t1))
	t1=time.time()
	for i in (0, nb_operations-1):
		flights_75_collection.find({"Dest":"JFK"})
		i+=1
	t2 = time.time()
	print("-Flights- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights- Reading Throughput time : ", nb_operations/(t2-t1))
	
#******* write data ********
def write_flights():
	t1=time.time()
	for i in (0, nb_operations-1):
		flights_collection.insert(
								{
								  "TaxiIn": 31,
								  "SecurityDelay": 0,
								  "DepTime": 901,
								  "DepDelay": 1,
								  "WeatherDelay": 0,
								  "CRSArrTime": 1750,
								  "id": "1",
								  "DayofMonth": 15,
								  "DayOfWeek": 6,
								  "TaxiOut": 22,
								  "Dest": "JFK",
								  "CRSElapsedTime": 350,
								  "ArrDelay": 0,
								  "AirTime": 299,
								  "CarrierDelay": 0,
								  "CRSDepTime": 900,
								  "Diverted": 0,
								  "Distance": 2586,
								  "UniqueCarrier": "DL",
								  "NASDelay": 0,
								  "Cancelled": 0,
								  "TailNum": "N645DL",
								  "Origin": "SFO",
								  "LateAircraftDelay": 0,
								  "Month": 12,
								  "ActualElapsedTime": 349,
								  "Year": 2007,
								  "ArrTime": 1750,
								  "CancellationCode": ""
								}
							)
		i+=1
	t2 = time.time()
	print("-Flights- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights- Writing Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		flights_25_collection.insert(
								{
								  "TaxiIn": 31,
								  "SecurityDelay": 0,
								  "DepTime": 901,
								  "DepDelay": 1,
								  "WeatherDelay": 0,
								  "CRSArrTime": 1750,
								  "id": "1",
								  "DayofMonth": 15,
								  "DayOfWeek": 6,
								  "TaxiOut": 22,
								  "Dest": "JFK",
								  "CRSElapsedTime": 350,
								  "ArrDelay": 0,
								  "AirTime": 299,
								  "CarrierDelay": 0,
								  "CRSDepTime": 900,
								  "Diverted": 0,
								  "Distance": 2586,
								  "UniqueCarrier": "DL",
								  "NASDelay": 0,
								  "Cancelled": 0,
								  "TailNum": "N645DL",
								  "Origin": "SFO",
								  "LateAircraftDelay": 0,
								  "Month": 12,
								  "ActualElapsedTime": 349,
								  "Year": 2007,
								  "ArrTime": 1750,
								  "CancellationCode": ""
								}
							)
		i+=1
	t2 = time.time()
	print("-Flights_25- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- Writing Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		flights_50_collection.insert(
								{
								  "TaxiIn": 31,
								  "SecurityDelay": 0,
								  "DepTime": 901,
								  "DepDelay": 1,
								  "WeatherDelay": 0,
								  "CRSArrTime": 1750,
								  "id": "1",
								  "DayofMonth": 15,
								  "DayOfWeek": 6,
								  "TaxiOut": 22,
								  "Dest": "JFK",
								  "CRSElapsedTime": 350,
								  "ArrDelay": 0,
								  "AirTime": 299,
								  "CarrierDelay": 0,
								  "CRSDepTime": 900,
								  "Diverted": 0,
								  "Distance": 2586,
								  "UniqueCarrier": "DL",
								  "NASDelay": 0,
								  "Cancelled": 0,
								  "TailNum": "N645DL",
								  "Origin": "SFO",
								  "LateAircraftDelay": 0,
								  "Month": 12,
								  "ActualElapsedTime": 349,
								  "Year": 2007,
								  "ArrTime": 1750,
								  "CancellationCode": ""
								}
							)
		i+=1
	t2 = time.time()
	print("-Flights_50- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- Writing Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		flights_75_collection.insert(
								{
								  "TaxiIn": 31,
								  "SecurityDelay": 0,
								  "DepTime": 901,
								  "DepDelay": 1,
								  "WeatherDelay": 0,
								  "CRSArrTime": 1750,
								  "id": "1",
								  "DayofMonth": 15,
								  "DayOfWeek": 6,
								  "TaxiOut": 22,
								  "Dest": "JFK",
								  "CRSElapsedTime": 350,
								  "ArrDelay": 0,
								  "AirTime": 299,
								  "CarrierDelay": 0,
								  "CRSDepTime": 900,
								  "Diverted": 0,
								  "Distance": 2586,
								  "UniqueCarrier": "DL",
								  "NASDelay": 0,
								  "Cancelled": 0,
								  "TailNum": "N645DL",
								  "Origin": "SFO",
								  "LateAircraftDelay": 0,
								  "Month": 12,
								  "ActualElapsedTime": 349,
								  "Year": 2007,
								  "ArrTime": 1750,
								  "CancellationCode": ""
								}
							)
		i+=1
	t2 = time.time()
	print("-Flights_75- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- Writing Throughput time : ", nb_operations/(t2-t1))

#******* order BY ********
def orderby_flights():
	t1=time.time()
	for i in (0, nb_operations-1):
		flights_collection.Account.find().sort("Year")
		i+=1
	t2 = time.time()
	print("-Flights- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		flights_25_collection.Account.find().sort("Year")
		i+=1
	t2 = time.time()
	print("-Flights_25- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		flights_50_collection.Account.find().sort("Year")
		i+=1
	t2 = time.time()
	print("-Flights_50- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		flights_75_collection.Account.find().sort("Year")
		i+=1
	t2 = time.time()
	print("-Flights_75- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- orderby Throughput time : ", nb_operations/(t2-t1))

############## Join
def join():
	t1=time.time()
	for i in (0, nb_operations-1):
		join = db.airports.aggregate([
						    {
						      "$lookup":
						        {
						          "from": "flights",
						          "localField": "id",
						          "foreignField": "Dest",
						          "as": "country"
						        }
						   }
						])
		# for i in join:
		# 	print(i)
		# i+=1
	
	t2=time.time()
	print("-Flights- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		join = db.airports.aggregate([
						    {
						      "$lookup":
						        {
						          "from": "flights_25",
						          "localField": "id",
						          "foreignField": "Dest",
						          "as": "country"
						        }
						   }
						])
		i+=1
	# for i in join:
	# 	print(i)
	t2=time.time()
	print("-Flights_25- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		join = db.airports.aggregate([
						    {
						      "$lookup":
						        {
						          "from": "flights_50",
						          "localField": "id",
						          "foreignField": "Dest",
						          "as": "country"
						        }
						   }
						])
		i+=1
	# for i in join:
	# 	print(i)
	t2=time.time()
	print("-Flights_50- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		join = db.airports.aggregate([
						    {
						      "$lookup":
						        {
						          "from": "flights_75",
						          "localField": "id",
						          "foreignField": "Dest",
						          "as": "country"
						        }
						   }
						])
		i+=1
	# for i in join:
	# 	print(i)

	t2=time.time()
	print("-Flights_75- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- orderby Throughput time : ", nb_operations/(t2-t1))

################################################## Couchbase
bucket_Airports = Bucket("couchbase://127.0.0.1:8091/Airports")
bucket_Flights_25 = Bucket("couchbase://127.0.0.1:8091/Flights_25")
bucket_Flights_50 = Bucket("couchbase://127.0.0.1:8091/Flights_50")
bucket_Flights_75 = Bucket("couchbase://127.0.0.1:8091/Flights_75")
bucket_Flights = Bucket("couchbase://127.0.0.1:8091/Flights")

def read_airports_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT * FROM Airports WHERE Name = 'Crownpoint'")
		i+=1
	# for row in bucket.n1ql_query(query):
	#     print(row)
	t2=time.time()
	print("-Airports- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Airports- Reading Throughput time : ", nb_operations/(t2-t1))

def write_airports_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("INSERT INTO Airports ( KEY, VALUE ) VALUES  ('New', { 'City': 'Alger','Name': 'Alger','Country': 'ALG','IATA/FAA': 'LG','Longitude': 33.2015961,'Latitude': 35.71765889,'id': '23632'} ) RETURNING * ;")
		i+=1
	t2=time.time()
	print("-Airports- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Airports- Writing Throughput time : ", nb_operations/(t2-t1))

def orderby_airports_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT id FROM Flights ORDER BY City")
		i+=1
	t2=time.time()
	print("-Airports- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Airports- orderby Throughput time : ", nb_operations/(t2-t1))

def read_Flights_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT * FROM Flights_25 WHERE Dest = 'JFK'")
		i+=1
	# for row in bucket_Flights_25.n1ql_query(query):
	#     print(row)
	t2=time.time()
	print("-Flights_25- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- Reading Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT * FROM Flights_50 WHERE Dest = 'JFK'")
		i+=1
	# for row in bucket.n1ql_query(query):
	#     print(row)
	t2=time.time()
	print("-Flights_50- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- Reading Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT * FROM Flights_75 WHERE Dest = 'JFK'")
		i+=1
	# for row in bucket.n1ql_query(query):
	#     print(row)
	t2=time.time()
	print("-Flights_75- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- Reading Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT * FROM Flights WHERE Dest = 'JFK'")
		i+=1
	# for row in bucket.n1ql_query(query):
	#     print(row)
	t2=time.time()
	print("-Flights- Reading latency time : ", (t2-t1)/nb_operations)
	print("-Flights- Reading Throughput time : ", nb_operations/(t2-t1))

def write_Flights_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("INSERT INTO Flights_25 ( KEY, VALUE )VALUES( 'New',{'TaxiIn': 31,'SecurityDelay': 0,'DepTime': 901,'DepDelay': 1,'WeatherDelay': 0,'CRSArrTime': 1750,'id': '1','DayofMonth': 15,'DayOfWeek': 6,'TaxiOut': 22,'Dest': 'JFK','CRSElapsedTime': 350,'ArrDelay': 0,'AirTime': 299,'CarrierDelay': 0,'CRSDepTime': 900,'Diverted': 0,'Distance': 2586,'UniqueCarrier': 'DL','NASDelay': 0,'Cancelled': 0,'TailNum': 'N645DL','Origin': 'SFO','LateAircraftDelay': 0,'Month': 12,'ActualElapsedTime': 349,'Year': 2007,'ArrTime': 1750,'CancellationCode': '} ) RETURNING * ;")
		i+=1
	t2=time.time()
	print("-Flights_25- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- Writing Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("INSERT INTO Flights_50 ( KEY, VALUE )VALUES( 'New',{'TaxiIn': 31,'SecurityDelay': 0,'DepTime': 901,'DepDelay': 1,'WeatherDelay': 0,'CRSArrTime': 1750,'id': '1','DayofMonth': 15,'DayOfWeek': 6,'TaxiOut': 22,'Dest': 'JFK','CRSElapsedTime': 350,'ArrDelay': 0,'AirTime': 299,'CarrierDelay': 0,'CRSDepTime': 900,'Diverted': 0,'Distance': 2586,'UniqueCarrier': 'DL','NASDelay': 0,'Cancelled': 0,'TailNum': 'N645DL','Origin': 'SFO','LateAircraftDelay': 0,'Month': 12,'ActualElapsedTime': 349,'Year': 2007,'ArrTime': 1750,'CancellationCode': '} ) RETURNING * ;")
		i+=1
	t2=time.time()
	print("-Flights_50- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- Writing Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("INSERT INTO Flights_75 ( KEY, VALUE )VALUES( 'New',{'TaxiIn': 31,'SecurityDelay': 0,'DepTime': 901,'DepDelay': 1,'WeatherDelay': 0,'CRSArrTime': 1750,'id': '1','DayofMonth': 15,'DayOfWeek': 6,'TaxiOut': 22,'Dest': 'JFK','CRSElapsedTime': 350,'ArrDelay': 0,'AirTime': 299,'CarrierDelay': 0,'CRSDepTime': 900,'Diverted': 0,'Distance': 2586,'UniqueCarrier': 'DL','NASDelay': 0,'Cancelled': 0,'TailNum': 'N645DL','Origin': 'SFO','LateAircraftDelay': 0,'Month': 12,'ActualElapsedTime': 349,'Year': 2007,'ArrTime': 1750,'CancellationCode': '} ) RETURNING * ;")
		i+=1
	t2=time.time()
	print("-Flights_75- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- Writing Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("INSERT INTO Flights ( KEY, VALUE )VALUES( 'New',{'TaxiIn': 31,'SecurityDelay': 0,'DepTime': 901,'DepDelay': 1,'WeatherDelay': 0,'CRSArrTime': 1750,'id': '1','DayofMonth': 15,'DayOfWeek': 6,'TaxiOut': 22,'Dest': 'JFK','CRSElapsedTime': 350,'ArrDelay': 0,'AirTime': 299,'CarrierDelay': 0,'CRSDepTime': 900,'Diverted': 0,'Distance': 2586,'UniqueCarrier': 'DL','NASDelay': 0,'Cancelled': 0,'TailNum': 'N645DL','Origin': 'SFO','LateAircraftDelay': 0,'Month': 12,'ActualElapsedTime': 349,'Year': 2007,'ArrTime': 1750,'CancellationCode': '} ) RETURNING * ;")
		i+=1
	t2=time.time()
	print("-Flights- Writing latency time : ", (t2-t1)/nb_operations)
	print("-Flights- Writing Throughput time : ", nb_operations/(t2-t1))

def orderby_Flights_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT id, Dest FROM Flights_25 ORDER BY Year")
		i+=1
	t2=time.time()
	print("-Flights_25- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT id, Dest FROM Flights_50 ORDER BY Year")
		i+=1
	t2=time.time()
	print("-Flights_50- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT id, Dest FROM Flights_75 ORDER BY Year")
		i+=1
	t2=time.time()
	print("-Flights_75- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- orderby Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT id, Dest FROM Flights ORDER BY Year")
		i+=1
	t2=time.time()
	print("-Flights- orderby latency time : ", (t2-t1)/nb_operations)
	print("-Flights- orderby Throughput time : ", nb_operations/(t2-t1))

def join_couchbase():
	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT a.City FROM Flights_25 f USE KEYS '100' JOIN Airports a ON KEYS f.Dest")
		i+=1
	t2=time.time()
	# for row in bucket_Flights_25.n1ql_query(query):
	#     print(row)
	print("-Flights_25- join latency time : ", (t2-t1)/nb_operations)
	print("-Flights_25- join Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0, nb_operations-1):
		query = N1QLQuery("SELECT a.City FROM Flights_50 f USE KEYS '100' JOIN Airports a ON KEYS f.Dest")
		i+=1
	t2=time.time()
	print("-Flights_50- join latency time : ", (t2-t1)/nb_operations)
	print("-Flights_50- join Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0,  nb_operations-1):
		query = N1QLQuery("SELECT a.City FROM Flights_75 f USE KEYS '100' JOIN Airports a ON KEYS f.Dest")
		i+=1
	t2=time.time()
	print("-Flights_75- join latency time : ", (t2-t1)/nb_operations)
	print("-Flights_75- join Throughput time : ", nb_operations/(t2-t1))

	t1=time.time()
	for i in (0,  nb_operations-1):
		query = N1QLQuery("SELECT a.City FROM Flights f USE KEYS '100' JOIN Airports a ON KEYS f.Dest")
		i+=1
	t2=time.time()
	print("-Flights- join latency time : ", (t2-t1)/nb_operations)
	print("-Flights- join Throughput time : ", nb_operations/(t2-t1))

print("MongoDB")
read_airport()
write_airport()
orderby_airport()
read_flights()
write_flights()
orderby_flights()

print("Couchbase")
read_airports_couchbase()
write_airports_couchbase()
orderby_airports_couchbase()
read_Flights_couchbase()
write_Flights_couchbase()
orderby_Flights_couchbase()
join_couchbase()