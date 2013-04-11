package com.renren.tailor.exec;


import java.io.Serializable;

import com.renren.tailor.model.RuleEngine;

public abstract class Task<T extends Serializable> implements Serializable {

  /**
   * task implementation
   */
  private static final long serialVersionUID = 1L;
  

   abstract void execute(String rule) throws Exception;
   
}
