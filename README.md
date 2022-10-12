# Big Data - Spark Analysis
## Introduction
In this repository, some apache spark applications are practiced and applied on different data sets with different approaches, for the sake of training and learning spark technology in Big Data.

## Technologies
In this project, I used these versions of tools:
1. Apache Spark Core 3.3.0
2. Apache Spark SQL 3.3.0
3. Scala version 2.12.15
4. postgresql 42.5.0
5. sbt version 1.7.2
6. Oracle Corporation Java 1.8.0_341

## Setup and pc specifications
This project run on intellij IDEA 2022.2.2 on my local computer with the following specifications:
* OS: Windows 10 Enterprise 21H2
* RAM: 16GB
* Processor :i7-1165G7
* SSD: INTEL SSDPEKNU512GZH

## Datasets
#### 1. sf_fire_calls: 
this dataset is mentioned in the LearningSpark2.0 textbool in chapter 3
Fire Calls-For-Service includes all fire units responses to calls. Each record includes the call number, incident number, address, unit identifier, call type, and disposition. All relevant time intervals are also included. Because this dataset is based on responses, and since most calls involved multiple units, there are multiple records for each call number. Addresses are associated with a block number, intersection or call box, not a specific address.
https://github.com/databricks/LearningSparkV2/tree/master/databricks-datasets/learning-spark-v2/sf-fire

#### 2. realestate: 
this dataset is included in the course of apache-spark-with-scala-hands-on-with-big-data.
it includes some information about houses, such as house age, location, price.

## Example of the applied analysis on the sf_fire_calls dataset:
#### [Query the data using the DataFrame API][./images/DFQuery.png]
