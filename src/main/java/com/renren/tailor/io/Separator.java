package com.renren.tailor.io;

public class Separator {

  private String next = "";  
  private String separator;  
  private String result;
  
  public Separator(String separator) {  
  this.separator = separator;  
  }  

  public String get() {  
  result = next;  
  next = separator;  
  return result;  
  }  
}
