package com.iiq.rtbEngine.enums;

public enum CampaignMatchStatus {
    UNMATCHED("unmatched"),
    CAPPED("capped");

    private final String value;

    private CampaignMatchStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
