package com.learnkafka.common;

import java.util.Enumeration;
import java.util.ResourceBundle;

public class DBConnection {
	public static void getProperties() {
		ResourceBundle res = ResourceBundle.getBundle("application");
		Enumeration<String> keys = res.getKeys();
		while(keys.hasMoreElements()) {
			String key = keys.nextElement();
			String value = res.getString(key);
			//System.out.println(key + ": " + value);
		}
	}
	
	
	
}
