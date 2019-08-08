package com.devoteam.kafka.kafkaproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserResource {
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final String TOPIC = "redistopic";
	
	@Bean
	public void post() throws InterruptedException {
		
		/*long startTime = System.currentTimeMillis();
		for(Integer i = 0; i < 1000000; i++) {
			
			String message = "Id is: " + i.toString();
			
			kafkaTemplate.send(TOPIC, message);
			
			//System.out.println("Sent message with content: " + message);
			
		}
		
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
		System.out.println("FINISHED! Duration: " + elapsedTime + " ms.");*/
		
		Integer i = 0;
		while (i < 100) {
			
			String message = "Id is: " + i.toString();
			kafkaTemplate.send(TOPIC, message);
			System.out.println("Sent message with content: " + message);
			Thread.sleep(600);
			i++;
		}
		
	}

}
