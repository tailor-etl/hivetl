package com.renren.tailor.exception;

public class JobConfigExcepton  extends Exception{

	private static final long serialVersionUID = 8958529775737026367L;
	
	private int type;//-101:输入文件不存在 -102:输出文件已经存在 

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public JobConfigExcepton(){
		super();
	}
	
	public JobConfigExcepton(String msg,int type){
		super(msg);
		setType(type);
	}
	
	public JobConfigExcepton(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
	
	public JobConfigExcepton(Exception e){
		super(e);
	}
}
