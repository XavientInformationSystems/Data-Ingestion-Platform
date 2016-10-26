package com.xavient.dip.common.exceptions;

public class DataIngestException extends Exception {

	private static final long serialVersionUID = 1138100039870399881L;

	public DataIngestException(String message) {
		super(message);
	}

	public DataIngestException(Throwable throwable) {
		super(throwable);
	}

	public DataIngestException(String message, Throwable throwable) {
		super(message, throwable);
	}

}
