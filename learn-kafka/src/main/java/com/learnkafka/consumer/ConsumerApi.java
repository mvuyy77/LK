package com.learnkafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.io.Resources;
import com.learnkafka.common.DBAccess;

public class ConsumerApi {
    
	public static void main(String[] args) throws IOException, SQLException {
		KafkaConsumer<String, String> employeeConsumer;
		//connect to DB
	    DBAccess m = new DBAccess("src/main/resources/application.properties");
		Connection myConnection = m.getConnection();
	
		//Read the Properties for Consumer
		//try(
		InputStream consumerConfig = Resources.getResource("consumer.properties").openStream();
			Properties properties = new Properties();
			properties.load(consumerConfig);
			employeeConsumer = new KafkaConsumer<>(properties);
		//}
		
		employeeConsumer.subscribe(Arrays.asList("EMPLOYEES"));
		Statement stmt = myConnection.createStatement();
		
		while(true) {
			ConsumerRecords<String, String> records = employeeConsumer.poll(100);

			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.offset() + "; " + record.value());
				int col1 = new Integer(record.value().split(",")[0]);
				String col2=new String(record.value().split(",")[1]);
				String col3=new String(record.value().split(",")[2]);
				String col4=new String(record.value().split(",")[3]);
				String col5=new String(record.value().split(",")[4]);
				String col6=new String(record.value().split(",")[5]);
				String data="insert into employees_temp(emp_no, birth_date, first_name, last_name, gender, hire_date) values ('"+col1+"','"+col2+"','"+col3+"','"+col4+"','"+col5+"','"+col6+"')";
				stmt.executeUpdate(data);
				System.out.println("----------------------------------");
				System.out.println("Following Record Updated in DB :-");
				System.out.println(record.value());
				System.out.println("----------------------------------");

			}
			employeeConsumer.close();
		}	

	}	
}

