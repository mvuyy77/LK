package com.learnkafka.producer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.io.Resources;
import com.learnkafka.common.DBAccess;
import com.learnkafka.common.Employee;

public class ProducerApi {    
	
	public static void main(String[] args) throws FileNotFoundException, InvalidPropertiesFormatException, IOException, SQLException {	
		KafkaProducer<String, String> producer;
		String msg = null;
	    String topic= "EMPLOYEES";
	    DBAccess m = new DBAccess("src/main/resources/application.properties");
		Connection myConnection = m.getConnection();
		
		//Read the Producer Properties
		InputStream producerConfig = Resources.getResource("producer.properties").openStream();
		Properties properties = new Properties();
		properties.load(producerConfig);
		producer = new KafkaProducer<>(properties);
	
		try {	
		    TestCallback callback = new TestCallback();
			String query = "select emp_no, birth_date, first_name, last_name, gender, hire_date from employees";
			if(myConnection != null) {
				Statement st = myConnection.createStatement();
				ResultSet rs = st.executeQuery(query);
				int counter = 0;
				while (rs.next())
			      {
					Employee emp = new Employee(rs.getString("emp_no"), rs.getString("birth_date"), rs.getString("first_name"), rs.getString("last_name"), rs.getString("gender"), rs.getString("hire_date"));
					msg = emp.getEmp_no()+","+emp.getBirthDate()+","+emp.getFirstName()+","+emp.getLastName()+","+emp.getGender()+","+emp.getHireDate();
					producer.send(new ProducerRecord<String, String>(topic, msg), callback).get();
					producer.flush();
					counter++;
					//if (counter > 10000) break;
			      }
			}
	    }catch (Throwable throwable) {
	    		System.out.println(throwable.getStackTrace());
	    }finally {
	    		producer.close();
			m.closeConnection(myConnection);			
	    }
	}
	
	//To display the information as to what Partition, offset # and topic information the data is being written to
	private static class TestCallback implements Callback {
	       @Override
	       public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	           if (e != null) {
	               System.out.println("Error while producing message to topic :" + recordMetadata);
	               e.printStackTrace();
	           } else {
	               String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
	               System.out.println(message);
	           }
	       }
	   }
}
