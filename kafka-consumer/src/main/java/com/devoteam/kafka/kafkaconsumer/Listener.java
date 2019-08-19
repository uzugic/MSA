package com.devoteam.kafka.kafkaconsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
	private boolean redisConnected;    //flag which shows if we have connection towards redis db
	private HashSet<Model> missingRecords;    //set of records which haven't been written successfully to redis
	
	@Value("${redis.host}")
    private String host;
	
	@Value("${redis.port}")
    private Integer port;
	
	@Value("${redis.password}")
    private String password;
	
	@PostConstruct
	public void init() {
		
		//upon bean creation initialize set of missing records and connection towards redis
		missingRecords = new HashSet<Model>();
		
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
	
	public void initializeRedis() {
		
		try {			
			jedis = new Jedis(host, port);    //try to connect to redis
			jedis.auth(password);
			redisConnected = true;    //set flag to true if successful
			System.out.println("Connection to redis successful!");
		}
		catch (Exception e) {
			//e.printStackTrace();
			redisConnected = false;   //set flag to false if not successful
			System.out.println("Connection to redis failed!");
			System.out.println("CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
		}
	}
	
	@KafkaListener(topics = "redistopic", groupId = "group_id")
	public void consume(String message) {
		
		Model m = parseJson(message);   //try to parse the incoming message
		
		if (m == null) {    //if parse was not successful, that means that record was invalid, exit the function
			return;
		}
			
		if(redisConnected) {   // are we connected to redis?
				
			if(!missingRecords.isEmpty()) {	  //if we are connected to redis, and missing records set is not empty
				insertMissingRecords();    //write the missing data to redis
			}
				
			insertRecord(m);    //attempt to write the new record to redis
			
		}
			
		else {   //if there is no connection towards redis
			missingRecords.add(m);   //add record to the missing records set
			System.out.println("Added to missing records! User id: " + m.getId());	
			initializeRedis();   //attempt to re-establish connection
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
	
	public void insertRecord(Model m) {   //function which inserts the record into redis 
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
			missingRecords.add(m);
			System.out.println("Added to missing records! User id: " + m.getId());	
			initializeRedis();   //if we lost the connection to redis meanwhile, insert the record to missing records set
			// and attempt to re-establish the connection to redis and update the flag
		}
	}
	
	public void insertMissingRecords() {    
		Iterator<Model> i = missingRecords.iterator();    //iterator which goes through entire set of missing records
		while (i.hasNext()) {	//as long as the set is not empty
		   Model md = i.next();   //get the next missing record
		   System.out.println("MISSING RECORD:");
		   insertRecord(md);   //insert the missing record to redis
		   i.remove();	//upon insertion, remove the record from set
		}
	}
	
	@PreDestroy
	public void delete() throws IOException {    //before we destroy the bean, close the connection towards redis
		jedis.close();
	}

}
