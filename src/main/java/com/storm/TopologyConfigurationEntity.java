package com.storm;

import java.io.Serializable;

public class TopologyConfigurationEntity implements Serializable {
	
	private String sourceKafkaTopic;
	private String cassandraKeyspace;
	private String cassandraTable;
	private String cassandraHost;
	private String zkConnectionString;
	private String kafkaBrokers;
	private String invalidKafkaTpoic;
	private String topologyName;

	public String getSourceKafkaTopic() {
		return sourceKafkaTopic;
	}

	public void setSourceKafkaTopic(String sourceKafkaTopic) {
		this.sourceKafkaTopic = sourceKafkaTopic;
	}

	public String getCassandraKeyspace() {
		return cassandraKeyspace;
	}

	public void setCassandraKeyspace(String cassandraKeyspace) {
		this.cassandraKeyspace = cassandraKeyspace;
	}

	public String getCassandraTable() {
		return cassandraTable;
	}

	public void setCassandraTable(String cassandraTable) {
		this.cassandraTable = cassandraTable;
	}

	public String getCassandraHost() {
		return cassandraHost;
	}

	public void setCassandraHost(String cassandraHost) {
		this.cassandraHost = cassandraHost;
	}

	public String getZkConnectionString() {
		return zkConnectionString;
	}

	public void setZkConnectionString(String zkConnectionString) {
		this.zkConnectionString = zkConnectionString;
	}

	public String getKafkaBrokers() {
		return kafkaBrokers;
	}

	public void setKafkaBrokers(String kafkaBrokers) {
		this.kafkaBrokers = kafkaBrokers;
	}

	public String getInvalidKafkaTpoic() {
		return invalidKafkaTpoic;
	}

	public void setInvalidKafkaTpoic(String invalidKafkaTpoic) {
		this.invalidKafkaTpoic = invalidKafkaTpoic;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}
	
}