package com.devoteam.kafka.kafkaproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserResource {
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final String TOPIC = "RedisTopic";
	
	@Bean
	public void post() {
		
		for(Integer i = 0; i < 1000; i++) {
			
			String message = "Id is: " + i.toString();
			
			kafkaTemplate.send(TOPIC, message);
			
			System.out.println("Sent message with content: " + message);
			
			try {
				Thread.sleep(400);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
