package test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class JedisTest {
	
	public static final int  DEFAULT_TIMEOUT      = 5000;
    public static final int    DEFAULT_REDIRECTIONS = 5;
	

	public static void main(String[] args) throws InterruptedException, IOException {
		
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.auth("mica123");
		
		String test = "lemi je govedo";
		
		String asd[] = test.split("jsade");
		
		System.out.println(asd[1]);
	
		/*Set<HostAndPort> connectionPoints = new HashSet<HostAndPort>();
		connectionPoints.add(new HostAndPort("10.0.200.232", 7000));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7001));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7002));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7003));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7004));
        connectionPoints.add(new HostAndPort("10.0.200.232", 7005));

        JedisCluster jedis = new JedisCluster(connectionPoints, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_REDIRECTIONS, new JedisPoolConfig());*/
		
		Integer rowsNum = 1000000; 
		
		//loadDB(rowsNum, jedis);
		//periodicAddition(rowsNum, 3600000, jedis);
		jedis.close();

	}
	
	public static void fetchUser(String id, Jedis jedis) {
		HashMap<String, String> res = (HashMap<String, String>) jedis.hgetAll(id);
		for(Entry<String, String> e: res.entrySet()) {
			System.out.println("RESULT - " + e.getKey() + ": " + e.getValue());
		}
	}
	
	public static void loadDB(Integer rowsNum, Jedis jedis) {
		System.out.println("INSERTION STARTED!");
		long startTime = System.currentTimeMillis();
		for(Integer i = 0; i < rowsNum; i++) {
			HashMap<String, String> hmap = new HashMap<String, String>();
			hmap.put("id", i.toString());
			hmap.put("username", "User" + i);
			hmap.put("password", "Password" + i);
			hmap.put("age", "27");
			hmap.put("gender", "TRIGGERED");
			hmap.put("attribute1", "attribute1Val");
			hmap.put("attribute2", "attribute2Val");
			hmap.put("attribute3", "attribute3Val");
			hmap.put("attribute4", "attribute4Val");
			hmap.put("attribute5", "attribute5Val");
			hmap.put("attribute6", "attribute6Val");
			jedis.hmset("user:" + i, hmap);
			//System.out.println("Added new hash with id: " + i.toString());
		}
		
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
		System.out.println("FINISHED! Duration: " + elapsedTime + " ms.");
	
	}
	
	public static void periodicAddition(Integer rowsNum, Integer duration, JedisCluster jedis) throws InterruptedException {
		Integer current = rowsNum;
		long now = System.currentTimeMillis();
		long end = now + duration;
		while(System.currentTimeMillis() < end) {
			HashMap<String, String> hmap = new HashMap<String, String>();
			hmap.put("id", current.toString());
			hmap.put("username", "User" + current);
			hmap.put("password", "Password" + current);
			hmap.put("age", "27");
			hmap.put("gender", "male");
			hmap.put("attribute1", "attribute1Val-" + current);
			hmap.put("attribute2", "attribute2Val-" + current);
			hmap.put("attribute3", "attribute3Val-" + current);
			hmap.put("attribute4", "attribute4Val-" + current);
			hmap.put("attribute5", "attribute5Val-" + current);
			hmap.put("attribute6", "attribute6Val-" + current);
			jedis.hmset("user:" + current, hmap);
			System.out.println("Added new user with id: " + current);
			current++;
			Thread.sleep(3000);
		}
	}

}
