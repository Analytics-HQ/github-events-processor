package com.analyticshq.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubEvent {

    @NotNull(message = "id cannot be null")
    @JsonProperty("id")
    private String id;

    @NotNull(message = "type cannot be null")
    @JsonProperty("type")
    private String type;

    @NotNull(message = "created_at cannot be null")
    @JsonProperty("created_at")
    private String createdAt;

    @NotNull(message = "public cannot be null")
    @JsonProperty("public")
    private boolean isPublic;

    @JsonProperty("actor_name")
    private String actorLogin; // Extracts actor.login

    @JsonProperty("repo_name")
    private String repoName; // Extracts repo.name

    @JsonProperty("ts")
    private String ts; // Timestamp when object is serialized

    @JsonIgnore
    private Map<String, Object> actor;

    @JsonIgnore
    private Map<String, Object> repo;

    @JsonIgnore
    private Map<String, Object> payload;

    @JsonIgnore
    private Map<String, Object> org;

    public GithubEvent() {
        this.ts = Instant.now().toString(); // Set timestamp when object is created
    }

    public String getId() { return id; }
    public String getType() { return type; }
    public String getCreatedAt() { return createdAt; }
    public boolean isPublic() { return isPublic; }
    public String getActorLogin() { return actorLogin; }
    public String getRepoName() { return repoName; }
    public String getTs() { return ts; }
    public Map<String, Object> getPayload() { return payload; }
    public Map<String, Object> getOrg() { return org; }

    public void setId(String id) { this.id = id; }
    public void setType(String type) { this.type = type; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }
    public void setPublic(boolean isPublic) { this.isPublic = isPublic; }

    @JsonProperty("actor")
    public void setActor(Object actor) {
        if (actor instanceof Map) {
            Map<?, ?> actorMap = (Map<?, ?>) actor;
            this.actorLogin = actorMap.get("login") instanceof String ? (String) actorMap.get("login") : null;
        } else {
            this.actorLogin = null;
        }
    }

    @JsonProperty("repo")
    public void setRepo(Object repo) {
        if (repo instanceof Map) {
            Map<?, ?> repoMap = (Map<?, ?>) repo;
            this.repoName = repoMap.get("name") instanceof String ? (String) repoMap.get("name") : null;
        } else {
            this.repoName = null;
        }
    }

    public void setPayload(Map<String, Object> payload) { this.payload = payload; }
    public void setOrg(Map<String, Object> org) { this.org = org; }
}
