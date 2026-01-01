#!/bin/bash

#Prerequisit for all the demos is Hadoop and Hive Stack is running

source ~/unset_jupyter.sh

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 tips_app.py
