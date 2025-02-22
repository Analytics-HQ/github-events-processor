package com.analyticshq.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubEvent {
    
    @NotNull(message = "id cannot be null")
    private String id;
    
    @NotNull(message = "type cannot be null")
    private String type;

    @NotNull(message = "created_at cannot be null")
    private String created_at;

    private String actor;
    private String repo;

    public String getId() { return id; }
    public String getType() { return type; }
    public String getCreatedAt() { return created_at; }
    
    public String getActor() { return actor; }
    public String getRepo() { return repo; }

    public void setId(String id) { this.id = id; }
    public void setType(String type) { this.type = type; }
    public void setCreatedAt(String created_at) { this.created_at = created_at; }
    public void setActor(String actor) { this.actor = actor; }
    public void setRepo(String repo) { this.repo = repo; }
}
