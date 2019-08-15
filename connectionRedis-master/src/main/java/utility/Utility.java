package utility;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Utility {

	@Value("${spring.redis.host}")
	private String redisHostName;

	@Value("${spring.redis.port}")
	private int redisPort;

	@Value("${spring.redis.password}")
	private String password;

	public String getRedisHostName() {
		return redisHostName;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public String getPassword() {
		return password;
	}
	
	

}
