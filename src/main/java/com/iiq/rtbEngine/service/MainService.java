package com.iiq.rtbEngine.service;

import com.iiq.rtbEngine.controller.DbManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class MainService {
    private DbManager dbManager;

    @Autowired
    MainService(DbManager dbmanager){
        dbManager=dbmanager;
    }


    //profileAttributeRequest
    public void processNewProfileAttribute(Integer profileId, Integer attributeId){
        dbManager.updateProfileAttribute(profileId, attributeId);
        updateProfileCampaigns(profileId, attributeId);
    }

    //bidRequest
    public String getBid(Integer profileId){
        return dbManager.getNextCampaign(profileId);
    }

    public void deleteProfileCache(Integer profileId){
        dbManager.deleteProfileCache(profileId);
    }

    private void updateProfileCampaigns(Integer profileId, Integer attributeId){
        if(dbManager.getAttributeCampaigns(attributeId).isEmpty())return;
        for(Integer campaignId: dbManager.getAttributeCampaigns(attributeId)){
            if(dbManager.getProfileCampaigns(profileId).contains(campaignId))continue;
            if (dbManager.getProfileAttributes(profileId).containsAll(dbManager.getCampaignAttributes(campaignId))) {
                dbManager.updateProfileCampaigns(profileId, campaignId);
            }
        }
    }


}
