package com.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraInsertBolt implements IRichBolt {

	Cluster cluster;
	private String cassandraKeyspace;
	private String cassandraTable;
	private String cassandraHost;
	TopologyConfigurationEntity topologyConfigEntity;
	private static transient CassandraConnection session;
	private static Session con;
	private OutputCollector outputCollector;
	private ArrayList<String> columnNames = new ArrayList<String>();
	private ArrayList<Object> fieldValues = new ArrayList<Object>();

	public CassandraInsertBolt(ArrayList<String> columnNames) {
		session = new CassandraConnection();
		this.columnNames = columnNames;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		this.cassandraKeyspace = (String) stormConf.get("cassandra.keyspace");
		this.cassandraTable = (String) stormConf.get("cassandra.table");
		this.cassandraHost = (String) stormConf.get("cassandra.host");
		con = session.getConnection(cassandraKeyspace, cassandraHost);
	}

	@Override
	public void execute(Tuple tuple) {
		System.out.println("In execute method of CassandraInsertBolt ************** >>> ");
		String statement = null;
		try {
			List<Object> tupleValues = tuple.getValues();
			JSONObject jsonObject = (JSONObject) tupleValues.get(0);

			ArrayList<Object> columnValues = new ArrayList<Object>();
			Object[] values = new Object[]{jsonObject.get("track_id"), jsonObject.get("artist"), jsonObject.get("title"), jsonObject.get("time_seg"), jsonObject.get("key"),jsonObject.get("mode"),jsonObject.get("year")}; 
			for (int i=0 ; i < values.length; i++) {
				columnValues.add(values[i]);
			}
			
			statement = insertRow(columnNames, columnValues, cassandraTable);
			con.execute(statement);
		} catch(Exception e){
			e.printStackTrace();
			System.out.println("Exception in inserting one statement >>> "+statement);
		} finally {
			outputCollector.ack(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

	public String insertRow(ArrayList<String> columnNames,  ArrayList<Object> fieldValues, String tableName) {
		String insertStatement = null;
		try {
			StringBuilder val = new StringBuilder();
			int noOfColumns = columnNames.size();
			insertStatement = "insert into " + tableName + " (";
			for(int i = 0; i <= noOfColumns - 1; i++) {
				if(i != noOfColumns - 1) {
					insertStatement = insertStatement + columnNames.get(i) + ", ";
					if (i <= 2) {
						val.append("'" + fieldValues.get(i) + "'" + ",");
					} else {
						val.append(fieldValues.get(i) + ",");
					}
				}
				else {
					insertStatement = insertStatement + columnNames.get(i) + ") ";
					val.append(fieldValues.get(i));
				}
			}
			insertStatement = insertStatement + " values (" +  val + ")";
			System.out.println("inserting row :- " + insertStatement );
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return insertStatement;	
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		con.close();
		session.close();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}