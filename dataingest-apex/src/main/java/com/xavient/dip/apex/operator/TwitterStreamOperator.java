package com.xavient.dip.apex.operator;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.xavient.dip.common.utils.FlatJsonConverter;

public class TwitterStreamOperator extends BaseOperator {

	@AutoMetric
	private long tuplesCount;
	@AutoMetric
	private long errorTuples;

	public final transient DefaultOutputPort<Object> errorPort = new DefaultOutputPort<>();
	public final transient DefaultOutputPort<Object[]> hdfsOutputPort = new DefaultOutputPort<>();
	public final transient DefaultOutputPort<Object[]> hBaseOutputPort = new DefaultOutputPort<>();
	// @InputPortFieldAnnotation(schemaRequired = true)
	public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>() {

		@Override
		public void process(Object record) {
			Object[] data = FlatJsonConverter.convertToValuesArray(new String((byte[]) record));
			hdfsOutputPort.emit(data);
			hBaseOutputPort.emit(data);
		}
	};

	@Override
	public void beginWindow(long windowId) {
		super.beginWindow(windowId);
		errorTuples = tuplesCount = 0;
	}

}
