package com.xavient.dip.apex.operator;

import org.apache.commons.lang3.StringUtils;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class HdfsSinkOperator extends AbstractFileOutputOperator<Object[]> {

	private String fileName;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	protected String getFileName(Object[] tuple) {
		return fileName;
	}

	@Override
	protected byte[] getBytesForTuple(Object[] tuple) {
		StringBuilder recordBuilder = new StringBuilder();
		for (Object e : (Object[]) tuple) {
			recordBuilder.append(e);
			recordBuilder.append("\\|");
		}
		return StringUtils.removeEnd(recordBuilder.toString(), "\\|").getBytes();
	}

}
