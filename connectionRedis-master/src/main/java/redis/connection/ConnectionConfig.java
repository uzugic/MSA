package redis.connection;

import javax.annotation.Resource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import utility.Utility;

@Configuration

public class ConnectionConfig {
     @Resource
	private Utility utils;
     
     @Bean

     Utility getUtility() {

 		Utility utility = new Utility();
 		return utility;
 	}

	JedisPool getJedisPool() {

		JedisPool jedisPool = new JedisPool(utils.getRedisHostName(), utils.getRedisPort());

		return jedisPool;
	}

	@Bean

	Jedis getJedis() {

		redis.clients.jedis.Jedis jedis = getJedisPool().getResource();
		return jedis;
	}

}
