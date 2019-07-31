package com.devoteam.kafka.kafkaconsumer;

import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;


@Service
public class Listener {
	
	private Jedis jedis;
	
	@PostConstruct
	public void init() {
		jedis = new Jedis("10.0.200.232", 6379);
		jedis.auth("mica123");
	}
	
	@KafkaListener(topics = "RedisTopic", groupId = "group_id")
	public void consume(String message) {
		
		System.out.println("Message has arrived: Content: " + message);
		String[] spt = message.split("Id is: ");
		String id = spt[1];
		HashMap<String, String> hmap = new HashMap<String, String>();
		hmap.put("id", id);
		hmap.put("username", "User" + id);
		hmap.put("password", "Password" + id);
		hmap.put("age", "27");
		hmap.put("gender", "TRIGGERED");
		hmap.put("attribute1", "attribute1Val");
		hmap.put("attribute2", "attribute2Val");
		hmap.put("attribute3", "attribute3Val");
		hmap.put("attribute4", "attribute4Val");
		hmap.put("attribute5", "attribute5Val");
		hmap.put("attribute6", "attribute6Val");
		jedis.hmset("user:" + id, hmap);
		
	}
	
	@PreDestroy
	public void delete() {
		jedis.close();
	}

}
