package com.iiq.rtbEngine.db;

import java.sql.SQLException;
import java.util.*;

import com.iiq.rtbEngine.models.CampaignConfig;
import com.iiq.rtbEngine.util.FilesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProfilesDao {
	@Autowired
	private H2DB h2Db;
	
	private static final String PROFILES_TABLE_NAME = "profiles";
	private static final String ATTRIBUTE_ID_COLUMN = "attribute_id";
	private static final String PROFILE_ID_COLUMN = "profile_id";
	private static final String CREATE_PROFILES_TABLE = "CREATE TABLE "+PROFILES_TABLE_NAME+ 
			"("+PROFILE_ID_COLUMN+" INTEGER not NULL, " + 
			ATTRIBUTE_ID_COLUMN + " INTEGER, " +  
			" PRIMARY KEY ( "+PROFILE_ID_COLUMN+ ", "+ATTRIBUTE_ID_COLUMN+"))";  
	private static final String UPDATE_STATMENT = "MERGE INTO "+PROFILES_TABLE_NAME+" KEY("+PROFILE_ID_COLUMN+", "+ATTRIBUTE_ID_COLUMN+") VALUES (%s, %s)";
	private static final String SELECT_STATEMENT = "SELECT * FROM "+PROFILES_TABLE_NAME+" where "+PROFILE_ID_COLUMN+" = %s";
	private static final String DELETE_STATEMENT = "DELETE FROM "+PROFILES_TABLE_NAME+" where "+PROFILE_ID_COLUMN+" = %s";
	private static final String SELECT_ALL_PROFILES_STATEMENT = "SELECT * FROM "+PROFILES_TABLE_NAME;
	
	public void createTable() {
		try {
			h2Db.executeUpdate(CREATE_PROFILES_TABLE);
		} catch (SQLException e) {
			System.out.println("Error while trying to create table "+PROFILES_TABLE_NAME);
			e.printStackTrace();
		}
	}
	
	public void updateTable(String profileId, String attributeId) {
		try {
			h2Db.executeUpdate(String.format(UPDATE_STATMENT, profileId, attributeId, profileId, attributeId));
		} catch (SQLException e) {
			System.out.println("Error while trying to update table "+PROFILES_TABLE_NAME+" with profileId="+profileId+" attributeId"+attributeId);
			e.printStackTrace();
		}
	}
	
	public Set<Integer> getProfileAttributes(int profileId) {

		Set<Integer> attributes = new HashSet<>();
		try {
			List<Map<String, String>> result = h2Db.executeQuery(String.format(SELECT_STATEMENT, profileId+""), ATTRIBUTE_ID_COLUMN);
			if(result == null)
				return attributes;
		
			for(Map<String, String> row : result) {
				Integer attributeId = Integer.parseInt(row.get(ATTRIBUTE_ID_COLUMN));
				attributes.add(attributeId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return attributes;
	}

	public Map<Integer, Set<Integer>> getAllProfileAttributes() {
		Map<Integer, Set<Integer>> res = new HashMap<>();

		try {
			List<Map<String, String>> result = h2Db.executeQuery(SELECT_ALL_PROFILES_STATEMENT, PROFILE_ID_COLUMN, ATTRIBUTE_ID_COLUMN);
			if(result == null)
				return Collections.emptyMap();

			for(Map<String, String> row : result) {
				Integer profileId = Integer.parseInt(row.get(PROFILE_ID_COLUMN));
				Integer attribId = Integer.parseInt(row.get(ATTRIBUTE_ID_COLUMN));
				if(!res.containsKey(profileId)){
					res.put(profileId, new HashSet<>());
				}
				res.get(profileId).add(attribId);
			}
		} catch (Exception e) {
			System.out.println("Error while trying to retrieve all profile attributes from DB");
			e.printStackTrace();
		}
		return res;
	}
}
