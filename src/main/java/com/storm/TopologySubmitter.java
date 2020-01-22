package com.storm;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
//import org.apache.storm.kafka.spout.KafkaSpout;
//import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class TopologySubmitter implements Serializable {

	private static Config config;
	private static String topologyName;
	private static String topicName;
	private static String zkConnString;
	private static String cassandraHost;
	private static String cassandraKeyspace;
	private static String cassandraTable;
	private static String cassandraBoltName;
	private static String cassandraBoltStream = "cassandraBoltStreamId";
	private transient static TopologyConfigurationEntity topologyConfigEntity;
	
    public static void main(String[] args) throws Exception {
    	try {
    		ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
	        topologyConfigEntity = objectMapper.readValue(new File("src/main/resources/songs_topology_yaml.yaml"), TopologyConfigurationEntity.class);
	        
	        createStormConfig();
	        
	        System.out.println("topicName >>>>> "+topicName);
	        System.out.println("topologyName >>>>> "+topologyName);
	
	        List<String> zkHostsList = new ArrayList<String>();
	        zkHostsList.add("localhost");
	        // Defining broker hosts for the configured zookeeper connection
	    	BrokerHosts hosts = new ZkHosts(zkConnString);
	    	SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName , "/" + topicName, "storm-consumer");
	    	spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	    	spoutConfig.zkPort = 2181;
	    	spoutConfig.zkServers = zkHostsList;

	        ArrayList<String> columnNames = new ArrayList<String>();
	        String[] names = {"track_id", "artist_name", "title", "time_signature", "key", "mode", "year"};
			for (int i=0 ; i < names.length ; i++) {
				columnNames.add(names[i]);
			}

	    	TopologyBuilder topologyBuilder = new TopologyBuilder();
	        topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig));
	        topologyBuilder.setBolt("validationbolt", new ValidationBolt(org.apache.storm.utils.Utils.DEFAULT_STREAM_ID), 1).shuffleGrouping("spout");
	        topologyBuilder.setBolt("cassandrabolt", new CassandraInsertBolt(columnNames)).shuffleGrouping("validationbolt"); 
	        
	        final LocalCluster localCluster = new LocalCluster();
	        localCluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
	        System.out.println("Topology " + topologyName + " submitted successfully >>> ");
    	} catch(Exception e){
    		System.out.println("Exception >>> "+e.getMessage());
    		e.printStackTrace();
    	}
    	Thread.sleep(10000);
    }

    public static void createStormConfig() throws Exception {
    	config = new Config();
    	config.setDebug(false);
    	config.setNumWorkers(1);
    	config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
//    	config.put("topologyConfigEntity", topologyConfigEntity);

        topologyName = topologyConfigEntity.getTopologyName();
        cassandraTable = topologyConfigEntity.getCassandraTable();
        cassandraKeyspace = topologyConfigEntity.getCassandraKeyspace();
        cassandraHost = topologyConfigEntity.getCassandraHost();
    	topicName = topologyConfigEntity.getSourceKafkaTopic();
    	zkConnString = topologyConfigEntity.getZkConnectionString();
        
    	config.put("topologyName", topologyName);
    	config.put("cassandra.host", cassandraHost);
    	config.put("cassandra.keyspace", cassandraKeyspace);
    	config.put("cassandra.table", cassandraTable);
    	config.put("topicName", topicName);
    	config.put("cassandra.host", topologyConfigEntity.getCassandraHost());
    	config.put("cassandra.keyspace", topologyConfigEntity.getCassandraKeyspace());
    }
    
    // Private method that sets Kafka properties
    private static Properties getProperties() {
    	// create instance for properties to access producer configs
        Properties props = new Properties();

        // Setting the below properties required for Kafka producer
        // Assign localhost id. 
        // Set acknowledgements for producer requests
        // If the request fails, the producer can automatically retry,
        // Specify buffer size in config
        // Reduce the no of requests less than 0
        // The buffer.memory controls the total amount of memory available to the producer for buffering.

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
    
}