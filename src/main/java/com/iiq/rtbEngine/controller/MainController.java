package com.iiq.rtbEngine.controller;

import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.iiq.rtbEngine.service.MainService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;


@RestController
public class MainController {

	private final MainService service;
	Logger logger = LoggerFactory.getLogger(MainController.class);
	private static final String ACTION_TYPE_VALUE = "act";
	private static final String ATTRIBUTE_ID_VALUE = "atid";
	private static final String PROFILE_ID_VALUE = "pid";

	@Autowired
	MainController(MainService mainService){
		this.service = mainService;
	}
	
	private enum UrlParam {
		ACTION_TYPE(ACTION_TYPE_VALUE),
		ATTRIBUTE_ID(ATTRIBUTE_ID_VALUE),
		PROFILE_ID(PROFILE_ID_VALUE),
		;
		
		private final String value;
		
		private UrlParam(String value) {
			this.value = value;
		}
		
		public String getValue() {
			return value;
		}
	}
	
	private enum ActionType {
		ATTRIBUTION_REQUEST(0),
		BID_REQUEST(1),
		;
		
		private int id;
		private static Map<Integer, ActionType> idToRequestMap = new HashMap<>();
		
		static {
			for(ActionType actionType : ActionType.values())
				idToRequestMap.put(actionType.getId(), actionType);
		}
		
		private int getId() {
			return this.id;
		}
		
		private ActionType(int id) {
			this.id = id; 
		}
		
		public static ActionType getActionTypeById(int id) {
			return idToRequestMap.get(id);
		}
	}
	
	@Autowired
	private DbManager dbManager;
	
	@PostConstruct
	public void init() {
		//initialize stuff after application finished start up
	}
	
	@GetMapping("/api")
	public void getRequest(HttpServletRequest request, HttpServletResponse response, 
			@RequestParam(name = ACTION_TYPE_VALUE,  required = true) int actionTypeId,
			@RequestParam(name = ATTRIBUTE_ID_VALUE, required = false) Integer attributeId,
			@RequestParam(name = PROFILE_ID_VALUE,   required = false) Integer profileId) {
		paramValidate(actionTypeId, attributeId, profileId, response);
		try{
			switch(ActionType.getActionTypeById(actionTypeId)) {
				case ATTRIBUTION_REQUEST:
					service.processNewProfileAttribute(profileId, attributeId);
					break;
				case BID_REQUEST:
					response.setContentType("text/html");
					OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream(), Charset.forName("UTF-8"));
					out.write(service.getBid(profileId));
					out.flush();
					break;
				default:
					throw new ResponseStatusException(
							HttpStatus.BAD_REQUEST, "unknown action type value");
			}
		}catch (Exception e){
			logger.error("error processing request with actionTypeId {}, error stacktrace: {}",
					actionTypeId,
					e.getStackTrace());
			throw new ResponseStatusException(
					HttpStatus.INTERNAL_SERVER_ERROR, "error processing request");
		}
	}

	@GetMapping("/deletecache")
	public void deleteRequest(HttpServletRequest request, HttpServletResponse response,
						   @RequestParam(name = PROFILE_ID_VALUE,   required = true) Integer profileId) {
		try{
			service.deleteProfileCache(profileId);
		}catch (Exception e){
			logger.error("error processing delete request, error stacktrace: {}",
					e.getStackTrace());
			throw new ResponseStatusException(
					HttpStatus.INTERNAL_SERVER_ERROR, "error processing request");
		}
	}

	private void paramValidate(int actionTypeId, Integer attributeId, Integer profileId, HttpServletResponse response){
		switch(ActionType.getActionTypeById(actionTypeId)) {
			case ATTRIBUTION_REQUEST:
				if(attributeId == null || profileId == null){
					throw new ResponseStatusException(
							HttpStatus.BAD_REQUEST, "attribute Id and profile Id required");
				}
				break;
			case BID_REQUEST:
				if(profileId == null){
					throw new ResponseStatusException(
							HttpStatus.BAD_REQUEST, "profile Id required");
				}
				break;
		}
	}
	
}
