package com.renren.tailor.io;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.Text;

public class SinkFlow implements Serializable{

  /**
   * job 输出
   */
  private static final long serialVersionUID = 1L;
  private static StringBuilder sb = new StringBuilder();
  
  /**
   * define separator for StringBuilder
   */
  private static Separator separator = new Separator(" ");
  
  
  public static Text sink(List<String> value){
    
    for(String field:value){
      if (field != null && !"".equals(field)) {  
        sb.append(separator.get()).append(field);  
    }  
      
    }
    
    return new  Text(sb.toString());
  }

 

}
