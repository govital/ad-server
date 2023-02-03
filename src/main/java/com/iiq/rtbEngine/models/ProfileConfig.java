package com.iiq.rtbEngine.models;

import java.io.*;
import java.util.*;
import com.iiq.rtbEngine.enums.CampaignMatchStatus;

public class ProfileConfig {
    private final Map<Integer, CampaignConfig> campaignConfigs;
    private Map<Integer, Integer> campaignServed;
    private List<Integer> priorityQueue;
    private Map<Integer, List<Integer>> priorityCampaigns;
    private boolean unmatched = false;
    private static final String BASE_PATH = "./resources/";
    private static final String PRIORITY_Q_DIR = "priorityQ";
    private static final String PRIORITY_CAMPAIGN_DIR = "priorityCampaign";
    private static final String SERVED_DIR = "served";
    private final String campaignServedFileName;
    private final String priorityCampaignsFileName;
    private final String priorityQueueFileName;


/**
 * An API for initializing the ProfileConfig object for a certain profile
 * @param campaignconfigs a map with key - campaingnId, value - CampaignConfig obj.
 * @return
 */
    public ProfileConfig(Integer profileid,Map<Integer, CampaignConfig> campaignconfigs){
        campaignServedFileName = BASE_PATH + SERVED_DIR +"/"+ profileid;
        priorityCampaignsFileName = BASE_PATH + PRIORITY_CAMPAIGN_DIR +"/"+ profileid;
        priorityQueueFileName = BASE_PATH + PRIORITY_Q_DIR +"/"+  profileid;
        if(campaignconfigs.isEmpty()){
            campaignConfigs = new HashMap<>();
            unmatched = true;
            return;
        }
        campaignConfigs = new HashMap<>(campaignconfigs);
        boolean priorityQueueSet = setPriorityQueue();
        boolean priorityCampaignsSet = setPriorityCampaigns();
        boolean CampaignServedSet = setCampaignServed();
        if(priorityQueueSet && priorityCampaignsSet && CampaignServedSet){
            return;
        }
        populateProfileConfig();
    }

    public void deleteCache(){
        campaignServed = new HashMap<>();
        File f = new File(campaignServedFileName);
        if (f.exists()){
            f.delete();
        }
        f = new File(priorityQueueFileName);
        if (f.exists()){
            f.delete();
        }
        f = new File(priorityCampaignsFileName);
        if (f.exists()){
            f.delete();
        }
        priorityCampaigns = new HashMap<>();
        populateProfileConfig();
    }

    private boolean setPriorityQueue(){
        File f = new File(priorityQueueFileName);
        if (f.exists()){
            loadFromFile(priorityQueueFileName);
            return true;
        }
        priorityQueue = new ArrayList();
        return false;
    }

    private boolean setPriorityCampaigns(){
        File f = new File(priorityCampaignsFileName);
        if (f.exists()){
            loadFromFile(priorityCampaignsFileName);
            return true;
        }
        priorityCampaigns = new HashMap<>();
        return false;
    }

    private boolean setCampaignServed(){
        File f = new File(campaignServedFileName);
        if (f.exists()){
            loadFromFile(campaignServedFileName);
            return true;
        }
        campaignServed = new HashMap<>();
        return false;
    }

    private void persist(String fileName){
        File file = new File(fileName);
        try {
            FileOutputStream f = null;
            f = new FileOutputStream(file);
            ObjectOutputStream s = null;
            s = new ObjectOutputStream(f);
            if(fileName.contains(PRIORITY_Q_DIR)){
                s.writeObject(priorityQueue);
            }
            else if(fileName.contains(PRIORITY_CAMPAIGN_DIR)){
                s.writeObject(priorityCampaigns);
            }
            else{
                s.writeObject(campaignServed);
            }
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadFromFile(String fileName){
        try {
            File file = new File(fileName);
            FileInputStream f = new FileInputStream(file);
            ObjectInputStream s = new ObjectInputStream(f);
            if(fileName.contains(PRIORITY_Q_DIR)){
                priorityQueue = (List<Integer>)s.readObject();
            }
            else if(fileName.contains(PRIORITY_CAMPAIGN_DIR)){
                priorityCampaigns = (Map<Integer,List<Integer>>)s.readObject();
            }
            else{
                campaignServed = (Map<Integer,Integer>) s.readObject();
            }
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addCampaignConfig(Integer campaignId, CampaignConfig campaignConfig){
        campaignConfigs.put(campaignId, campaignConfig);
        if(campaignServed == null)
            campaignServed = new HashMap<>();
        campaignServed.put(campaignId, 0);
        persist(campaignServedFileName);
        int campaignPriority = campaignConfig.getPriority();
        if(!priorityQueue.contains(campaignPriority)){
            priorityQueue.add(campaignPriority);
            priorityQueue.sort(Comparator.reverseOrder());
            persist(priorityQueueFileName);
        }
        if(!priorityCampaigns.containsKey(campaignPriority)){
            priorityCampaigns.put(campaignPriority, new ArrayList<>());
            persist(priorityCampaignsFileName);
        }
        if(!priorityCampaigns.get(campaignPriority).contains(campaignId)){
            priorityCampaigns.get(campaignPriority).add(campaignId);
        }
        Collections.sort(priorityCampaigns.get(campaignPriority));
        persist(priorityCampaignsFileName);
    }

    public String serveNextCampaign(){
        if(unmatched)return CampaignMatchStatus.UNMATCHED.getValue();
        if(priorityQueue.isEmpty())return CampaignMatchStatus.CAPPED.getValue();
        int priority = priorityQueue.get(0);
        int campaignId = priorityCampaigns.get(priority).get(0);
        if(!campaignServed.containsKey(campaignId))
            campaignServed.put(campaignId, 0);
        campaignServed.put(campaignId, campaignServed.get(campaignId)+1);
        persist(campaignServedFileName);
        if(campaignConfigs.get(campaignId).getCapacity()<=campaignServed.get(campaignId)){
            priorityCampaigns.get(priority).remove(0);
            persist(priorityCampaignsFileName);
        }
        if(priorityCampaigns.get(priority).isEmpty()){
            priorityQueue.remove(0);
            persist(priorityQueueFileName);
            priorityCampaigns.remove(priority);
            persist(priorityCampaignsFileName);
        }
        return String.valueOf(campaignId);
    }

    private void populateProfileConfig(){
        for (Map.Entry<Integer, CampaignConfig> entry : campaignConfigs.entrySet()){
            addCampaignConfig(entry.getKey(), entry.getValue());
        }
    }



}
