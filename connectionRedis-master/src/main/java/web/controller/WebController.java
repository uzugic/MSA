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

	@GetMapping(value = "/readItems/{id}")

	public ResponseEntity<HashMap<String, String>> findItem(@PathVariable("id") final String id) {

		HashMap<String, String> res = (HashMap<String, String>) jedis.hgetAll("user:" + id);
		if (res.isEmpty()) {
			return new ResponseEntity(null, HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity(res, HttpStatus.OK);

	}

	@PostConstruct

	void auth() {

		jedis.auth(utils.getPassword());
	}
}
