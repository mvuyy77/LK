package com.learnkafka.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;


public class DBAccess {
	public String dbms;
	public String dbName; 
	public String userName;
	public String password;
	public String urlString;
	  
	private String driver;
	private String serverName;
	private int portNumber;
	private Properties prop;

	public DBAccess(String fileName) throws FileNotFoundException, InvalidPropertiesFormatException, IOException {
		super();
		this.setProperties(fileName);
	}

	private void setProperties(String fileName) throws FileNotFoundException, InvalidPropertiesFormatException, IOException {
		this.prop = new Properties();
		File file = new File(fileName);
		FileInputStream fis = new FileInputStream(file);
		prop.load(fis);
		
		this.dbms = this.prop.getProperty("DBMS");
		this.driver = this.prop.getProperty("DB_DRIVER");
		this.dbName = this.prop.getProperty("DB_NAME");
		this.userName = this.prop.getProperty("DB_USER_NAME");
		this.password = this.prop.getProperty("DB_PASSWORD");
		this.serverName = this.prop.getProperty("DB_SERVER_NAME");
		this.portNumber = Integer.parseInt(this.prop.getProperty("DB_PORT_NUMBER"));
		
		System.out.println("Set the following properties:");
		System.out.println("dbms: " + dbms);
		System.out.println("driver: " + driver);
		System.out.println("dbName: " + dbName);
		System.out.println("userName: " + userName);
		System.out.println("serverName: " + serverName);
		System.out.println("portNumber: " + portNumber);
	}	

	public Connection getConnection() throws SQLException {
		    Connection conn = null;
		    Properties connectionProps = new Properties();
		    connectionProps.put("user", this.userName);
		    connectionProps.put("password", this.password);
		    
		    String currentUrlString = null;
		    if (this.dbms.equals("mysql")) {
		      currentUrlString = "jdbc:" + this.dbms + "://" + this.serverName + ":" + this.portNumber + "/";
		      conn = DriverManager.getConnection(currentUrlString, connectionProps);
		      
		      this.urlString = currentUrlString + this.dbName;
		      conn.setCatalog(this.dbName);
			  System.out.println("Connected to database");
		    } 
		    return conn;
	}
	
	public void closeConnection(Connection connArg) {
	    System.out.println("Releasing all open resources & Terminating Database Connection!!!");
	    try {
	      if (connArg != null) {
	        connArg.close();
	        connArg = null;
	      }
	    } catch (SQLException sqle) {
	    		sqle.printStackTrace();
	    }
	  }
		
}
