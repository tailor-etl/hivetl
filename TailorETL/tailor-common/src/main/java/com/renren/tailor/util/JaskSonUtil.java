package com.renren.tailor.util;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;



public class JaskSonUtil {

	private static final ObjectMapper	objectMapper		= new ObjectMapper();
	
	
	public static ObjectMapper getObjectMapper(){
		return objectMapper;
	}
	
	
	
	public static <T> T readValueAsObjFromStr(String str,Class<T> t){
		T tt=null;
		try {
			// tt= objectMapper.readValue(str, new TypeReference<T>(){});
			 tt= objectMapper.readValue(str, t);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tt;
	}
}
