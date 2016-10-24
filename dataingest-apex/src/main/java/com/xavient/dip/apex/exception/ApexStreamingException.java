package com.xavient.dip.apex.exception;

public class ApexStreamingException extends Exception {

  private static final long serialVersionUID = 1138100039870399881L;

  public ApexStreamingException(String message) {
    super(message);
  }

  public ApexStreamingException(Throwable throwable) {
    super(throwable);
  }

  public ApexStreamingException(String message, Throwable throwable) {
    super(message, throwable);
  }


}
