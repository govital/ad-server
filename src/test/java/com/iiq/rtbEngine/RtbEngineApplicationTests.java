package com.iiq.rtbEngine;

import io.restassured.RestAssured;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import java.io.File;
import java.util.Date;
import java.util.Random;

import static io.restassured.RestAssured.baseURI;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RtbEngineApplicationTests {

	@LocalServerPort
	private int port;

	@Before
	public void setUp() throws Exception {
		RestAssured.port = port;
	}

	@Test
	void contextLoads() {
	}

	@Test
	public void ProfileAttributeRequeTest(){
		given().when().get( "http://localhost:"+port+"/api?act=0&pid=4&atid=2").then().statusCode(200);
	}

	@Test
	public void BidRequestTest(){
		String resp = given().when().get( "http://localhost:"+port+"/api?act=1&pid=3").then().statusCode(200).and().extract().body().asString();
		assertEquals(resp, "capped");
	}

	@Test
	public void FullFllowTest(){
		given().when().get( "http://localhost:"+port+"/deletecache?pid=0").then().statusCode(200);
		given().when().get( "http://localhost:"+port+"/api?act=0&pid=0&atid=20").then().statusCode(200);
		given().when().get( "http://localhost:"+port+"/api?act=0&pid=0&atid=21").then().statusCode(200);
		given().when().get( "http://localhost:"+port+"/api?act=0&pid=0&atid=22").then().statusCode(200);
		String resp = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("103", resp);
		String resp1 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("103", resp1);
		String resp2 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("102", resp2);
		String resp3 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("102", resp3);
		String resp4 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("101", resp4);
		String resp5 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("101", resp5);
		String resp6 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("101", resp6);
		String resp7 = given().when().get( "http://localhost:"+port+"/api?act=1&pid="+0).then().statusCode(200).and().extract().body().asString();
		assertEquals("capped", resp7);
	}

}
