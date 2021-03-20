# Updating-Partitioned-Data-Lake-using-Stream-and-Kafka

## _Distributing accurate and timely data over distances_

<a href="https://www.linkedin.com/in/alandanque"> Author: Alan Danque </a>

<a href="https://adanque.github.io/">Click here to go back to Portfolio Website </a>

![A remote image](https://adanque.github.io/assets/img/DataLake.jpg)

Replicating data onto data lakes accurately and timely over distances using Kafka.

## Pythonic Libraries Used in this project
- threading
- shutil
- os
- glob
- numpy
- pandas
- pyarrow
- dask
- datetime
- spark

## Repo Folder Structure


└───results

    └───stream

		└───input
		
			└───accelerations
				
				└───partitions...
		
		└───output

		└───staging		

			└───accelerations
			
			└───locations			

## Python Files 

| File Name  | Description |
| ------ | ------ |
| stream_data.py | Load parquet partitioned data with timestamp |
| stream_data_consumer.py | Pub sub Consumer queue topic watcher |
| stream_data_producer.py | Loads partitioned parquet data changes on time factors |

