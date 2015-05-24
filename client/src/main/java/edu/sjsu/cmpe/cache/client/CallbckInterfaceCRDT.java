package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;

public interface CallbckInterfaceCRDT {

    void putSuccess (HttpResponse<JsonNode> response, String serverUrl);
    void getSuccess (HttpResponse<JsonNode> response, String serverUrl);

    void putFailed (Exception e);
    void getFailed (Exception e);
}