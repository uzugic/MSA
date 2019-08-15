package web.controller;

import java.util.HashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import redis.clients.jedis.Jedis;
import utility.Utility;

@RestController

@PropertySource("application.properties")
@Service
//@EnableCircuitBreaker
public class WebController {

	@Autowired
	private Jedis jedis;
	@Autowired
	private Utility utils;

	@GetMapping(value = "/readItems")

	public HashMap<String, String> readItems() {

		HashMap<String, String> res = (HashMap<String, String>) jedis.hgetAll("user:2");
		return res;
	}

	@GetMapping(value = "/readItems/{id}")

	public HashMap<String, String> findItem(@PathVariable("id") final String id) {

		HashMap<String, String> res = (HashMap<String, String>) jedis.hgetAll("user:" + id);

		return res;

	}

	@PostMapping("/createItems")
	public ResponseEntity<HashMap<String, String>> createItems() {

		for (Integer i = 0; i < 10; i++) {
			HashMap<String, String> hmap = new HashMap<>();
			hmap.put("id", i.toString());
			hmap.put("username", "User" + i);
			hmap.put("password", "Password" + i);
			hmap.put("age", "27");
			hmap.put("gender", "male");
			hmap.put("attribute1", "attribute1Val");
			hmap.put("attribute2", "attribute2Val");
			hmap.put("attribute3", "attribute3Val");
			hmap.put("attribute4", "attribute4Val");
			hmap.put("attribute5", "attribute5Val");
			hmap.put("attribute6", "attribute6Val");
			jedis.hmset("user:" + i, hmap);

		}
		return new ResponseEntity<HashMap<String, String>>(HttpStatus.CREATED);
	}

	@PostConstruct

	void auth() {

		jedis.auth(utils.getPassword());
	}
}
