package org.illalabs.rss;

public class FeedDetails {

    private String url;
    private Integer pollIntervalMillis;
    private Long lastPolled;

    public FeedDetails(String string, String pollInterval) {
        this.setUrl(string);
        this.setPollIntervalMillis(Integer.parseInt(pollInterval));
    }

    public FeedDetails() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getPollIntervalMillis() {
        return pollIntervalMillis;
    }

    public void setPollIntervalMillis(Integer pollIntervalMillis) {
        this.pollIntervalMillis = pollIntervalMillis;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return sb.append("[").append(url).append(":")
                .append(pollIntervalMillis).append("]").toString();
    }

    public Long getLastPolled() {
        return lastPolled;
    }

    public void setLastPolled(Long lastPolled) {
        this.lastPolled = lastPolled;
    }
}
