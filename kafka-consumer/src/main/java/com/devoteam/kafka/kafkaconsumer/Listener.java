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
		
		jedis = new Jedis(host, port);
		jedis.auth(password);
		
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
	public void consume(String message) {
		
		//System.out.println("Message has arrived: Content: " + message);
		String[] spt = message.split("Id is: ");
		String id = spt[1];
		if (id != null) {
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
		
	}
	
	@PreDestroy
	public void delete() throws IOException {
		jedis.close();
	}

}
