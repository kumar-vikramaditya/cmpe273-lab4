package edu.sjsu.cmpe.cache.client;

import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
	 private final String server_url;
	    private CallbckInterfaceCRDT callbk;
	    public DistributedCacheService(String serverUrl) {
	        this.server_url = serverUrl;
	    }
	    public DistributedCacheService(String serverUrl, CallbckInterfaceCRDT callbk) {
	        this.server_url = serverUrl;
	        this.callbk = callbk;
	    }
	    @Override
	    public String get(long key) {
	        Future<HttpResponse<JsonNode>> future = Unirest.get(this.server_url + "/cache/{key}")
	                .header("accept", "application/json")
	                .routeParam("key", Long.toString(key))
	                .asJsonAsync(new Callback<JsonNode>() {
	                    public void failed(UnirestException e) {
	                        callbk.getFailed(e);
	                    }
	                    public void completed(HttpResponse<JsonNode> response) {
	                        callbk.getSuccess(response, server_url);
	                    }
	                    public void cancelled() {
	                        System.out.println("Request Cancel...");
	                    }

	                });

	        return null;
	    }

	  
	    @Override
	    public void put(long key, String value) {
	        Future<HttpResponse<JsonNode>> future = Unirest.put(this.server_url + "/cache/{key}/{value}")
	                .header("accept", "application/json")
	                .routeParam("key", Long.toString(key))
	                .routeParam("value", value)
	                .asJsonAsync(new Callback<JsonNode>() {

	                    public void failed(UnirestException e) {
	                        callbk.putFailed(e);
	                    }

	                    public void completed(HttpResponse<JsonNode> response) {
	                        callbk.putSuccess(response, server_url);
	                    }

	                    public void cancelled() {
	                        System.out.println("Request Cancel...");
	                    }

	                });
	    }

	    @Override
	    public void delete(long key) {
	        HttpResponse<JsonNode> response = null;
	        try {
	            response = Unirest
	                    .delete(this.server_url + "/cache/{key}")
	                    .header("accept", "application/json")
	                    .routeParam("key", Long.toString(key))
	                    .asJson();
	        } catch (UnirestException e) {
	            System.err.println(e);
	        }

	        System.out.println("response is " + response);

	        if (response == null || response.getCode() != 204) {
	            System.out.println("Delete fail...");
	        } else {
	            System.out.println("Deleted " + key + " from " + this.server_url);
	        }

	    }
}
