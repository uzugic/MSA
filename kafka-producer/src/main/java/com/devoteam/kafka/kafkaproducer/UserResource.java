package com.devoteam.kafka.kafkaproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class UserResource {
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final String TOPIC = "redistopic";
	
	@Bean
	public void post() throws InterruptedException, JsonProcessingException {
		

		/*for(Integer i = 0; i < 1000000; i++) {
			
			String message = "Id is: " + i.toString();
			
			kafkaTemplate.send(TOPIC, message);
			
		}*/
		
		Integer i = 1;
		while (i <= 1000) {
			
			Model m = new Model();
			m.setId("user:" + i);
			m.setUsername("User" + i);
			m.setPassword("Password" + i);
			m.setAge(26);
			m.setGender("TRIGGERED");
			m.setAttribute1("attr1:" + i);
			m.setAttribute2("attr2:" + i);
			m.setAttribute3("attr3:" + i);
			m.setAttribute4("attr4:" + i);
			m.setAttribute5("attr5:" + i);
			m.setAttribute6("attr6:" + i);
			
			ObjectMapper objectMapper = new ObjectMapper();
			String json = objectMapper.writeValueAsString(m);

			kafkaTemplate.send(TOPIC, json);
			System.out.println("MESSAGE SENT!: " + json);
			//Thread.sleep(500);
			i++;
		}
		
	}

}
