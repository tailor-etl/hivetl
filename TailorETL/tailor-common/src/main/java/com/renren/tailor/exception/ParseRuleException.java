package com.renren.tailor.exception;

public class ParseRuleException extends Exception{
	private static final long serialVersionUID = 1L;

	public ParseRuleException(){
		super();
	}
	
	public ParseRuleException(String msg){
		super(msg);
	}
	
	public ParseRuleException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
	
	public ParseRuleException(Exception e){
		super(e);
	}
}