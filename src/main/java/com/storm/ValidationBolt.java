package com.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ValidationBolt extends BaseRichBolt {

	private TopologyConfigurationEntity topologyConfigEntity;
	private String cassandraBoltStream;
	private String topologyName;
	private OutputCollector outputCollector;

	ValidationBolt(String cassandraBoltStream) {
		this.cassandraBoltStream = cassandraBoltStream;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;		
		this.topologyName = (String) stormConf.get("topologyName");
	}

	@Override
	public void execute(Tuple tuple) {
		String inputLine = tuple.getString(0);
		boolean isJsonValid = false;
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject = (JSONObject) jsonParser.parse(inputLine);
			
			if (jsonObject.containsKey("track_id"))
				isJsonValid = true;
	
			if (isJsonValid) {
				List dataList = new ArrayList();
				dataList.add(jsonObject);
				outputCollector.emit(cassandraBoltStream, tuple, dataList);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			outputCollector.ack(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declareStream(org.apache.storm.utils.Utils.DEFAULT_STREAM_ID, new Fields(new String[]{"jsonFields"}));
	}

}