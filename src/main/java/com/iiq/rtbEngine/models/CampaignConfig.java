package com.iiq.rtbEngine.models;

public class CampaignConfig {
	private int priority;
	private int capacity;
	
	public CampaignConfig(int priority, int capacity) {
		this.priority = priority;
		this.capacity = capacity;
	}

	public int getPriority() {
		return priority;
	}

	public int getCapacity() {
		return capacity;
	}
	
}