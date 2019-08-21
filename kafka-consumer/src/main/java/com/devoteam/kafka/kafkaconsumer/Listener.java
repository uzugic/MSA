package com.devoteam.kafka.kafkaconsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;


@Service
public class Listener {
	
	private Jedis jedis; 
	
	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
	@Value("${redis.host}")
    private String host;
	
	@Value("${redis.port}")
    private Integer port;
	
	@Value("${redis.password}")
    private String password;
	
	@Value("${redis.retry.interval.ms}")
    private Integer retryInterval;
	
	@PostConstruct
	public void init() throws InterruptedException {
		
		//upon bean creation initialize set of missing records and connection towards redis
		initializeRedis();
		//cluster setup, tbd
		/*Set<HostAndPort> connectionPoints = new HashSet<HostAndPort>();
		connectionPoints.add(new HostAndPort("10.0.200.232", 7000));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7001));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7002));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7003));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7004));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7005));

        jedis = new JedisCluster(connectionPoints);*/
	}
	
	public void initializeRedis() throws InterruptedException {
		
		try {			
			jedis = new Jedis(host, port);    //try to connect to redis
			jedis.auth(password);
			kafkaListenerEndpointRegistry.start();   //enable message consumption
			System.out.println("Connection to redis successful!");
		}
		catch (Exception e) {	//if connecting to redis was not successful
			//e.printStackTrace();
			kafkaListenerEndpointRegistry.stop();  //stop message consumption from kafka
			System.out.println("Connection to redis failed!");
			System.out.println("CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			Thread.sleep(retryInterval);	//attempt to reconnect every X seconds, specified in properties file
			System.out.println("ATTEMPTING TO RECONNECT TO REDIS");
			initializeRedis();
		}
	}
	
	@KafkaListener(topics = "${kafka.topic}", id = "kafkalistener", groupId = "${kafka.group.id}")
	public void consume(String message) throws InterruptedException  {	
			
		Model m = parseJson(message);   //try to parse the incoming message
		
		if (m != null) {    //if parse was successful, try to insert the record into redis
			insertRecord(m);
		}
		
	}
	
	public Model parseJson(String message) {
		try {
			ObjectMapper objectMapper = new ObjectMapper();		 
			Model m = objectMapper.readValue(message, Model.class);	//deserialize the JSON message we received, create model object
			return m;
		}
		catch (Exception e) {
			System.out.println("CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			System.out.println("JSON parse failed due to invalid record!");	//if parse was unsuccessful, record is not valid
			return null;
		}
	}
	
	public void insertRecord(Model m) throws InterruptedException {   //function which inserts the record into redis 
		HashMap<String, String> hmap = new HashMap<String, String>();  //hashmap is used to insert a new hash to redis
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
		try {
			jedis.hmset(m.getId(), hmap);   //add a new hash to redis
			System.out.println("Successfully written to Redis! User id: " + m.getId());
		}
		catch (Exception e) {
			System.out.println("CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			initializeRedis();   //if we lost the connection to redis, attempt to reconnect
		}
	}
	
	
	@PreDestroy
	public void delete() throws IOException {    //before we destroy the bean, close the connection towards redis
		jedis.close();
	}

}
