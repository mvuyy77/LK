package com.learnkafka.producer;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;
import com.learnkafka.common.DBAccess;
import com.learnkafka.common.Employees;

public class ProducerAvroApi {    
	
	public static void main(String[] args) throws Exception {	
		
		KafkaProducer<String, Employees> producer;
	    String topic= "INFO";
	    DBAccess m = new DBAccess("src/main/resources/application.properties");
		Connection myConnection = m.getConnection();
	    
	    try(InputStream props = Resources.getResource("producer.properties").openStream()){
	    		Properties properties = new Properties();
		    properties.load(props);
		    producer = new KafkaProducer<>(properties);
	    }
	    try {			
			String query = "select emp_no, birth_date, first_name, last_name, gender, hire_date from employees";
			if(myConnection != null) {
				Statement st = myConnection.createStatement();
				ResultSet rs = st.executeQuery(query);
				
				while (rs.next())
			    {
					Employees emp = new Employees();    
					emp.setEmpNo(rs.getInt("emp_no"));
					emp.setBirthDate(rs.getString("birth_date"));
					emp.setFirstName(rs.getString("first_name"));
					emp.setLastName(rs.getString("last_name"));
					emp.setGender(rs.getString("gender"));
					emp.setHireDate(rs.getString("hire_date"));
					producer.send(new ProducerRecord<String, Employees>(topic, emp.getEmpNo().toString(), emp));	
			    }
			}
	    }catch (Throwable throwable) {
	    		System.out.println("I AM HERE");
	    		System.out.println(throwable.getStackTrace());
	    }finally {
	    		producer.close();
			m.closeConnection(myConnection);			

	    }
	}
}
