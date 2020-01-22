package com.storm;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class PopulateKafkaTopic {

    public static void main(String[] args) throws Exception {

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        TopologyConfigurationEntity configEntity = objectMapper.readValue(new File("src/main/resources/songs_topology_yaml.yaml"), TopologyConfigurationEntity.class);
        String kafkaTopic = configEntity.getSourceKafkaTopic();

        List<JSONObject> jsonMessages = getJsonMessagesFromCSV();
        
        Producer<Long, String> kafkaProducer = new KafkaProducer<>(getProperties());
        String message = "";
//        for (int i=0; i<jsonMessages.size(); i++) {
        for (int i=0; i<1000; i++) {
        	message = jsonMessages.get(i).toString();
        	kafkaProducer.send(new ProducerRecord<Long, String>(kafkaTopic, null, message));
        }
        kafkaProducer.close();
        System.out.println("All messages are sent to the kafka topic successfully ... ");
    }

    // Private method that reads a csv file and generates Jsons
    private static List<JSONObject> getJsonMessagesFromCSV() throws JSONException, IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		InputStream inputStream = classLoader.getResourceAsStream("songs_data.csv");
		InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(streamReader);
	    String line =  null;

	    String[] keyList = new String[] {"track_id", "artist", "title", "loudness", "tempo", "time_sig", "key", "mode", "duration", "song_hotness", "year"};

	    List<JSONObject> jsonObjectsList = new ArrayList<JSONObject>();

	    while((line=reader.readLine()) != null) {
	    	JSONObject jsonObject = new JSONObject();
	        String jsonValues[] = line.split(",");
	        
	        for(int i=0; i<jsonValues.length; i++) {
	        	if (i<3 || (i >=5 && i <= 7) || i==10)
	        		jsonObject.put(keyList[i], jsonValues[i]);
	        }
	        jsonObjectsList.add(jsonObject);
	    }
	    
    	return jsonObjectsList;
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