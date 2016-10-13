package com.xavient.dip.common.exceptions;

public class DiPCommonException extends Exception {

  private static final long serialVersionUID = -1940899613156599256L;
  
  public DiPCommonException(String message) {
    super(message);
  }

  public DiPCommonException(Throwable throwable) {
    super(throwable);
  }

  public DiPCommonException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
