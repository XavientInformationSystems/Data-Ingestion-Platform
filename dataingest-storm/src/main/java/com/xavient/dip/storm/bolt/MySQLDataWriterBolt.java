package com.xavient.dip.storm.bolt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.dip.storm.utils.DataSourceFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MySQLDataWriterBolt extends DataIngestionBolt {

	private static final long serialVersionUID = 1552882008838885198L;
	private static final Logger LOG = LoggerFactory.getLogger(MySQLDataWriterBolt.class);
	private QueryRunner queryRunner;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.batchSize = Math.min(100, rankMaxThreshold);
		queryRunner = new QueryRunner(
				DataSourceFactory.getDataSource((Map<String, String>) stormConf.get("dbProperties")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	protected void finishBatch() {
		List<Tuple> tuples = new ArrayList<Tuple>();
		queue.drainTo(tuples);
		try {
			if (CollectionUtils.isNotEmpty(tuples)) {
				int t = 0;
				List<String> fields = getFileds(tuples.get(0));
				Object[][] data = new Object[tuples.size()][];
				for (Tuple tuple : tuples) {
					Object[] value = new Object[] { tuple.getValueByField(fields.get(0)),
							tuple.getValueByField(fields.get(1)), tuple.getValueByField(fields.get(1)) };
					data[t] = value;
					t++;
				}
				queryRunner.batch(buildQuery(tuples.get(0).getStringByField("tableName"), fields), data);
			}
		} catch (Exception e) {
			LOG.error("Exception occurred while executing query", e);
		}

	}

	private List<String> getFileds(Tuple tuple) {
		List<String> fields = new ArrayList<>();
		Iterator<String> itr = tuple.getFields().iterator();
		while (itr.hasNext()) {
			String field = itr.next();
			if (!"tableName".equals(field))
				fields.add(field);
		}
		return fields;
	}

	private static String buildQuery(String tableName, List<String> fields) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO ");
		builder.append(tableName);
		builder.append(" (");
		for (String field : fields) {
			builder.append(field);
			builder.append(",");
		}
		builder = new StringBuilder(StringUtils.removeEnd(builder.toString(), ","));
		builder.append(") ");
		builder.append(" VALUES ");
		builder.append(" (");
		for (int i = 0; i < fields.size(); i++) {
			builder.append(" ?,");
		}
		builder = new StringBuilder(StringUtils.removeEnd(builder.toString(), ","));
		builder.append(") ");
		builder.append(" ON DUPLICATE KEY UPDATE ");
		builder.append(fields.get(1));
		builder.append("=");
		if ("tweets_location".equals(tableName)) {
			builder.append(fields.get(1));
			builder.append("+");
		}
		builder.append(" ?");
		return builder.toString();
	}

}
