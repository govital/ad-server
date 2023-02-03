package com.iiq.rtbEngine.controller;

import com.iiq.rtbEngine.db.CampaignsConfigDao;
import com.iiq.rtbEngine.db.CampaignsDao;
import com.iiq.rtbEngine.db.ProfilesDao;
import com.iiq.rtbEngine.models.CampaignConfig;
import com.iiq.rtbEngine.models.ProfileConfig;
import com.iiq.rtbEngine.util.FilesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.iiq.rtbEngine.enums.CampaignMatchStatus;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;


@Service
public class DbManager {

	private Map<Integer, Set<Integer>> attribCampaignsMap;
	private Map<Integer, Set<Integer>> profileCampaignsMap;
	private Map<Integer, ProfileConfig> profileConfigMap;
	private Map<Integer, ReentrantLock> profileLockMap;
	
	@Autowired
	CampaignsConfigDao campaignsConfigDao;
	
	@Autowired
	CampaignsDao campaignsDao;
	
	@Autowired
	ProfilesDao profilesDao;
	
	private static final String CSV_DELIM = ",";
	private static final String BASE_PATH = "./resources/";	//should be D:/ on AWS workspace
	private static final String CAMPAIGNS_TABLE_INITIALIZATION_FILE = "campaigns_init.csv";
	private static final String ATTRIB_CAMPAIGNS_INIT_FILE = "atrib_campaigns_init.csv";
	private static final String PROFILE_CAMPAIGNS_INIT_FILE = "profile_campaigns_init.csv";
	private static final String PROFILE_CONFIGS_INIT_FILE = "profile_configs_init.csv";
	private static final String CAMPAIGNS_CAPACITY_TABLE_INITIALIZATION_FILE = "campaign_config_init.csv";
	private static final String PROFILES_TABLE_INITIALIZATION_FILE = "profiles_init.csv";


	@PostConstruct
	public void init() {
		attribCampaignsMap= new HashMap<>();
		profileCampaignsMap= new HashMap<>();
		profileConfigMap = new HashMap<>();
		profileLockMap = new HashMap<>();
		//populate Campaigns table from file
		initCampaignsTable();
		//populate Campaign capacity table from file
		initCampaignCapacityTable();
		//init Profile table & populate from file
		initProfilesTable();
		//populate AttribCampaignsMap
		initAttribCampaignsMap();
		initProfileCampaignsMap();
		initProfileConfigMap();
	}

	private void initAttribCampaignsMap() {
		if(fileLoad(ATTRIB_CAMPAIGNS_INIT_FILE))return;
		for (Map.Entry<Integer,List<Integer>> entry : campaignsDao.getAllCampaignAttributes().entrySet()){
			for(Integer atrib: entry.getValue()){
				if(!attribCampaignsMap.containsKey(atrib)){
					attribCampaignsMap.put(atrib, new HashSet<>());
				}
				attribCampaignsMap.get(atrib).add(entry.getKey());
			}
		}
		persistMap(attribCampaignsMap, ATTRIB_CAMPAIGNS_INIT_FILE);
	}

	private void persistMap(Map toPersist, String name){
		File file = new File(BASE_PATH + name);
		FileOutputStream f = null;
		try {
			f = new FileOutputStream(file);
			ObjectOutputStream s = null;
			s = new ObjectOutputStream(f);
			s.writeObject(toPersist);
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Map<Integer, Object> loadFromPersistence(String name){
		Map<Integer, Object> fileObj = null;
		try {
			File file = new File(name);
			FileInputStream f = new FileInputStream(file);
			ObjectInputStream s = new ObjectInputStream(f);
			fileObj = (HashMap<Integer, Object>) s.readObject();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileObj;
	}

	private boolean fileLoad(String fileName){
		File f = new File(fileName);
		if (f.exists()){
			switch (fileName){
				case PROFILE_CONFIGS_INIT_FILE:
					for(Map.Entry<Integer,Object> obj: loadFromPersistence(PROFILE_CONFIGS_INIT_FILE).entrySet()){
						profileConfigMap.put(obj.getKey(), (ProfileConfig) obj.getValue());
					}
				case ATTRIB_CAMPAIGNS_INIT_FILE:
					for(Map.Entry<Integer,Object> obj: loadFromPersistence(ATTRIB_CAMPAIGNS_INIT_FILE).entrySet()){
						attribCampaignsMap.put(obj.getKey(), (Set<Integer>)obj.getValue());
					}
				case PROFILE_CAMPAIGNS_INIT_FILE:
					for(Map.Entry<Integer,Object> obj: loadFromPersistence(PROFILE_CAMPAIGNS_INIT_FILE).entrySet()){
						profileCampaignsMap.put(obj.getKey(), (Set<Integer>)obj.getValue());
					}
			}
			return true;
		}
		return false;
	}

	private void initProfileCampaignsMap() {
		if(fileLoad(PROFILE_CAMPAIGNS_INIT_FILE))return;
		Map<Integer, Set<Integer>> allProfileAttributes = profilesDao.getAllProfileAttributes();
		if (allProfileAttributes.isEmpty()) return;
		for (Map.Entry<Integer, Set<Integer>> profileAttribEntry : allProfileAttributes.entrySet()) {
			Set<Integer> profilePossibleCampaigns = new HashSet();
			for (Integer atrib : profileAttribEntry.getValue()) {
				if(attribCampaignsMap.containsKey(atrib)){
					profilePossibleCampaigns.addAll(attribCampaignsMap.get(atrib));
				}
			}
			for(Integer possibleCampaign: profilePossibleCampaigns){
				Set<Integer> possibleCampaignAttribs = campaignsDao.getCampaignAttributes(possibleCampaign);
				if(!profileAttribEntry.getValue().containsAll(possibleCampaignAttribs))continue;
				if(!profileCampaignsMap.containsKey(profileAttribEntry.getKey())){
					profileCampaignsMap.put(profileAttribEntry.getKey(), new HashSet());
				}
				profileCampaignsMap.get(profileAttribEntry.getKey()).add(possibleCampaign);
			}
		}
		persistMap(profileCampaignsMap, PROFILE_CAMPAIGNS_INIT_FILE);
	}

	private void initProfileConfigMap() {
		Map<Integer, CampaignConfig> allCampaignConfigs = campaignsConfigDao.getAllCampaignsConfigs();
		for (Map.Entry<Integer, Set<Integer>> profileCampaigns : profileCampaignsMap.entrySet()) {
			Map<Integer, CampaignConfig> campaignconfigs = new HashMap<>();
			for(Integer campaignId: profileCampaigns.getValue()){
				if(allCampaignConfigs.containsKey(campaignId) && allCampaignConfigs.get(campaignId) != null){
					campaignconfigs.put(campaignId, allCampaignConfigs.get(campaignId));
				}
			}
			ProfileConfig profileConfig = new ProfileConfig(profileCampaigns.getKey(),campaignconfigs);
			profileConfigMap.put(profileCampaigns.getKey(), profileConfig);
		}
	}

	private void initCampaignsTable() {
		// create table in DB
		campaignsDao.createTable();

		// read initialization file
		List<String> lines = FilesUtil.readLinesFromFile(BASE_PATH + CAMPAIGNS_TABLE_INITIALIZATION_FILE);

		// insert campaigns capacities into DB
		for (String line : lines) {
			String[] values = line.split(CSV_DELIM);
			campaignsDao.updateTable(values[0], values[1]);
		}
	}

	private void initCampaignCapacityTable() {
		
		// create table in DB
		campaignsConfigDao.createTable();

		// read initialization file
		List<String> lines = FilesUtil.readLinesFromFile(BASE_PATH + CAMPAIGNS_CAPACITY_TABLE_INITIALIZATION_FILE);

		// insert campaigns capacities into DB
		for (String line : lines) {
			String[] values = line.split(CSV_DELIM);
			campaignsConfigDao.updateTable(values[0], values[1], values[2]);
		}
	}
	
	private void initProfilesTable() {
		// create table in DB
		profilesDao.createTable();

		// read initialization file
		List<String> lines = FilesUtil.readLinesFromFile(BASE_PATH + PROFILES_TABLE_INITIALIZATION_FILE);

		// insert campaigns capacities into DB
		for (String line : lines) {
			String[] values = line.split(CSV_DELIM);
			profilesDao.updateTable(values[0], values[1]);
		}
	}
	
	/*****************************************************************************************************************************************
	 ****************************************						DEVELOPER API							*********************************
	 ****************************************************************************************************************************************/
	
	/**
	 * An API for getting the capacity that is configured for a certain campaign
	 * @param campaignId
	 * @return the capacity configured for the given campaign, or null in case the given campaignId does not
	 * have any campaign configuration 
	 */
	public Integer getCampaignCapacity(int campaignId) {
		return campaignsConfigDao.getCampaignCapacity(campaignId);
	}
	
	/**
	 * An API for getting the priority that is configured for a certain campaign
	 * @param campaignId
	 * @return the priority configured for the given campaign, or null in case the given campaignId does not
	 * have any campaign configuration 
	 */
	public Integer getCampaignPriority(int campaignId) {
		return campaignsConfigDao.getCampaignPriority(campaignId);
	}
	
	/**
	 * An API for getting all the attributes that a certain campaign is targeting
	 * @param campaignId
	 * @return a set of all the attributes that the given campaign targets, or an empty Set if the given
	 * campaignId does not have any campaign configuration
	 */
	public Set<Integer> getCampaignAttributes(int campaignId) {
		return campaignsDao.getCampaignAttributes(campaignId);
	}
	
	/**
	 * An API for getting configuration object for a certain campaign
	 * @param campaignId
	 * @return a CampaignConfig object containing the configuration entities for the given campaignId, 
	 * or null in case campaignId does not have any campaign configuration
	 */
	public CampaignConfig getCampaignConfig(int campaignId) {
		return campaignsConfigDao.getCampaignConfig(campaignId);
	}
	
	/**
	 * An API for getting all the attributes that match a certain profile
	 * @param profileId
	 * @return a set of all the profile IDs that match the given profile, or an empty set in case the given profile
	 * does not have any attribute IDs that match
	 */
	public Set<Integer> getProfileAttributes(int profileId) {
		return profilesDao.getProfileAttributes(profileId);
	}

	public void deleteProfileCache(int profileId) {
		if(profileConfigMap.containsKey(profileId)){
			profileConfigMap.get(profileId).deleteCache();
		}
	}
	
	/**
	 * An API for updating Profiles table in DB
	 * @param profileId
	 * @param attributeId
	 */
	public void updateProfileAttribute(int profileId, int attributeId) {
		profilesDao.updateTable(profileId +"", attributeId+"");
		FilesUtil.writeLineToFile(BASE_PATH + PROFILES_TABLE_INITIALIZATION_FILE, profileId+CSV_DELIM+attributeId);
	}

	/**
	 * An API for retrieving from DB all the campaign attributes for every campaign.
	 * @return a Map s.t. key = campaign ID, value = set of campaign attribute IDs
	 */
	public Map<Integer, List<Integer>> getAllCampaignAttributes() {
		return campaignsDao.getAllCampaignAttributes();
	}

	/**
	 * An API for retrieving from DB the campaign configuration for every campaign.
	 * @return a Map s.t. key = campaign ID, value = campaign configuration object
	 */
	public Map<Integer, CampaignConfig> getAllCampaignsConfigs() {
		return campaignsConfigDao.getAllCampaignsConfigs();
	}

	public void updateProfileCampaigns(Integer profileId, Integer campaignId) {
		profileCampaignsMap.get(profileId).add(campaignId);
		persistMap(profileCampaignsMap, PROFILE_CAMPAIGNS_INIT_FILE);
		if(!profileConfigMap.containsKey(profileId)){
			Map<Integer, CampaignConfig> campaignconfigs = new HashMap<>();
			campaignconfigs.put(campaignId, campaignsConfigDao.getCampaignConfig(campaignId));
			ProfileConfig profileConfig = new ProfileConfig(profileId, campaignconfigs);
			profileConfigMap.put(profileId, profileConfig);
		}else{
			profileConfigMap.get(profileId).addCampaignConfig(campaignId,campaignsConfigDao.getCampaignConfig(campaignId));
		}

	}

	public Set<Integer> getProfileCampaigns(Integer profileId) {
		if(!profileCampaignsMap.containsKey(profileId)){
			profileCampaignsMap.put(profileId, new HashSet());
		}
		return profileCampaignsMap.get(profileId);
	}

	public Set<Integer> getAttributeCampaigns(Integer attributeId) {
		if(!attribCampaignsMap.containsKey(attributeId)){
			return Collections.emptySet();
		}
		return attribCampaignsMap.get(attributeId);
	}

	public String getNextCampaign(Integer profileId) {
		if(!profileConfigMap.containsKey(profileId))
			return CampaignMatchStatus.UNMATCHED.getValue();
		return perform(profileId);
	}

	public String perform(Integer profileId) {
		ReentrantLock lock;
		String nextCampaign;
		ProfileConfig profileConfig = profileConfigMap.get(profileId);
		if (!profileLockMap.containsKey(profileId)){
			profileLockMap.put(profileId, new ReentrantLock());
		}
		lock = profileLockMap.get(profileId);
		lock.lock();
		try {
			nextCampaign = profileConfig.serveNextCampaign();
		} finally {
			lock.unlock();
		}
		return nextCampaign;
	}

}
