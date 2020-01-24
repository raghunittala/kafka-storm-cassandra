# kafka-storm-cassandra
Codebase that can populate a Apache kafka topic with JSON data and process them using Apache storm, do some transformations and finally store into Apache cassandra data store

# Populating the source kafka topic with data.
The source kafka topic is configured in "songs_topology_yaml.yaml" file available under /src/main/resources folder.

PopulateKafkaTopic.java will populate data to the source kafka topic.
