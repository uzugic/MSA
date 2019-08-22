package com.devoteam.kafka.kafkaconsumer;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
	//private JedisCluster jedisCluster;
	
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
	
	@Value("${logging.file}")
    private String loggingFile;
	
	private static final Logger LOGGER = Logger.getLogger(Listener.class.getName());
	
	@PostConstruct
	public void init() throws InterruptedException, SecurityException, IOException {
		
		//upon bean creation initialize logger handler (output file)
		LOGGER.setUseParentHandlers(false);
		setLoggerHandler();
		LOGGER.log(Level.INFO, "Kafka consumer initialized!");
		//initialize connection towards redis
		initializeRedis();
		
		//cluster setup, usage to be discussed
		/*HashSet<HostAndPort> connectionPoints = new HashSet<HostAndPort>();
		connectionPoints.add(new HostAndPort("10.0.200.232", 7000));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7001));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7002));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7003));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7004));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7005));

        jedisCluster = new JedisCluster(connectionPoints);*/
	}
	
	public void initializeRedis() throws InterruptedException {
		
		try {			
			jedis = new Jedis(host, port);    //try to connect to redis
			jedis.auth(password);
			kafkaListenerEndpointRegistry.start();   //enable message consumption
			LOGGER.log(Level.INFO, "Connection to redis successful!");
		}
		catch (Exception e) {	//if connecting to redis was not successful
			jedis.close();   //close session
			kafkaListenerEndpointRegistry.stop();  //stop message consumption from kafka
			LOGGER.log(Level.SEVERE, "Connection to redis failed!");
			LOGGER.log(Level.SEVERE, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			Thread.sleep(retryInterval);	//attempt to reconnect every X seconds, specified in properties file
			LOGGER.log(Level.INFO, "ATTEMPTING TO RECONNECT TO REDIS");
			initializeRedis();	//try to reconnect
		}
	}
	
	public void setLoggerHandler() throws SecurityException, IOException {
		
		FileHandler handler = new FileHandler(loggingFile, true);
		handler.setFormatter(new SimpleFormatter() {
            private static final String format = "[%1$tF %1$tT] [%2$-7s] %3$s %n";

            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format,
                        new Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        lr.getMessage()
                );
            }
        });
		LOGGER.addHandler(handler);
		ConsoleHandler chandler = new ConsoleHandler();
		LOGGER.addHandler(chandler);
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
			LOGGER.log(Level.INFO, "Record parsed successfully. Record id: " + m.getId());
			return m;
		}
		catch (Exception e) {
			LOGGER.log(Level.WARNING, "JSON parse failed due to invalid record!");	//if parse was unsuccessful, record is not valid, ignore it
			LOGGER.log(Level.WARNING, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
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
			LOGGER.log(Level.INFO, "Record successfully written to Redis! Record id: " + m.getId());
		}
		catch (Exception e) {
			LOGGER.log(Level.SEVERE, "CAUSE: "  + e.getCause() + "; ERROR MESSAGE - " + e.getMessage());
			initializeRedis();   //if we lost the connection to redis, attempt to reconnect
		}
	}
	
	
	@PreDestroy
	public void delete() throws IOException {    //before we destroy the bean, close the connection towards redis
		jedis.close();
	}

}
