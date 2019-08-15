package com.devoteam.kafka.kafkaconsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;


@Service
public class Listener {
	
	private Jedis jedis;
	
	@Value("${redis.host}")
    private String host;
	
	@Value("${redis.port}")
    private Integer port;
	
	@Value("${redis.password}")
    private String password;
	
	@PostConstruct
	public void init() {
		
		try {			
			jedis = new Jedis(host, port);
			jedis.auth(password);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Set<HostAndPort> connectionPoints = new HashSet<HostAndPort>();
		connectionPoints.add(new HostAndPort("10.0.200.232", 7000));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7001));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7002));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7003));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7004));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7005));

        jedis = new JedisCluster(connectionPoints);*/
	}
	
	@KafkaListener(topics = "redistopic", groupId = "group_id")
	public void consume(String message) throws JsonParseException, JsonMappingException, IOException {
		
		System.out.println("Message has arrived: Content: " + message);
		
		ObjectMapper objectMapper = new ObjectMapper();
		 
		Model m = objectMapper.readValue(message, Model.class);
		
		HashMap<String, String> hmap = new HashMap<String, String>();
		hmap.put("id", m.getId());
		hmap.put("username", m.getUsername());
		hmap.put("password", m.getPassword());
		hmap.put("age", m.getAge().toString());
		hmap.put("gender", m.getGender());
		hmap.put("attribute1", m.getAttribute1());
		hmap.put("attribute2", m.getAttribute2());
		hmap.put("attribute3", m.getAttribute3());
		hmap.put("attribute4", m.getAttribute4());
		hmap.put("attribute5", m.getAttribute5());
		hmap.put("attribute6", m.getAttribute6());
		jedis.hmset(m.getId(), hmap);

	}
	
	@PreDestroy
	public void delete() throws IOException {
		jedis.close();
	}

}
