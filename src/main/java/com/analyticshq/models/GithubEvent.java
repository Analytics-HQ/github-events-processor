package com.analyticshq.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubEvent {
    
    @NotNull(message = "id cannot be null")
    public String id;
    
    @NotNull(message = "type cannot be null")
    public String type;

    @NotNull(message = "created_at cannot be null")
    public String created_at;

    public String actor;
    public String repo;

    public String getId() { return id; }
    public String getType() { return type; }
    public String getCreatedAt() { return created_at; }
}
