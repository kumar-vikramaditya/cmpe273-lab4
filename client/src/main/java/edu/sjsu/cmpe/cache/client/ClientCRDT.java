package edu.sjsu.cmpe.cache.client;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.*;
import java.lang.InterruptedException;
import java.io.*;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.http.options.Options;


public class ClientCRDT implements CallbckInterfaceCRDT {

    private ConcurrentHashMap<String, CacheServiceInterface> servers_list;
    private ArrayList<String> server_success;
    private ConcurrentHashMap<String, ArrayList<String>> result_dict;

    private static CountDownLatch cntDwnLatch;
    

    public ClientCRDT() {

        servers_list = new ConcurrentHashMap<String, CacheServiceInterface>(3);
        CacheServiceInterface cache0 = new DistributedCacheService("http://localhost:3000", this);
        CacheServiceInterface cache1 = new DistributedCacheService("http://localhost:3001", this);
        CacheServiceInterface cache2 = new DistributedCacheService("http://localhost:3002", this);
        servers_list.put("http://localhost:3000", cache0);
        servers_list.put("http://localhost:3001", cache1);
        servers_list.put("http://localhost:3002", cache2);
    }

    // Callbacks
    @Override
    public void putFailed(Exception e) {
        System.out.println("Unable to process the request...");
        cntDwnLatch.countDown();
    }

    @Override
    public void putSuccess(HttpResponse<JsonNode> response, String serverUrl) {
        int getCode = response.getCode();
        System.out.println("Put success ------> " + getCode + " on server ------>" + serverUrl);
        server_success.add(serverUrl);
        cntDwnLatch.countDown();
    }

    @Override
    public void getFailed(Exception e) {
        System.out.println("Get failed...");
        cntDwnLatch.countDown();
    }

    @Override
    public void getSuccess(HttpResponse<JsonNode> response, String serverUrl) {

        String value = null;
        if (response != null && response.getCode() == 200) {
            value = response.getBody().getObject().getString("value");
                System.out.println("The received value from server ---->" + serverUrl + "is ---->" + value);
            ArrayList ServerWithValue = result_dict.get(value);
            if (ServerWithValue == null) {
                ServerWithValue = new ArrayList(3);
            }
            ServerWithValue.add(serverUrl);

            // Save Arraylist of servers into dictResults
            result_dict.put(value, ServerWithValue);
        }

        cntDwnLatch.countDown();
    }



    public boolean put(long key, String value) throws InterruptedException {
        server_success = new ArrayList(servers_list.size());
        cntDwnLatch = new CountDownLatch(servers_list.size());

        for (CacheServiceInterface cache : servers_list.values()) {
            cache.put(key, value);
        }

        cntDwnLatch.await();

        boolean isSuccess = Math.round((float)server_success.size() / servers_list.size()) == 1;

        if (! isSuccess) {
            // Send delete for the same key
            delete(key, value);
        }
        return isSuccess;
    }

    public void delete(long key, String value) {

        for (final String serverUrl : server_success) {
            CacheServiceInterface server = servers_list.get(serverUrl);
            server.delete(key);
        }
    }
    public String get(long key) throws InterruptedException {
        result_dict = new ConcurrentHashMap<String, ArrayList<String>>();
        cntDwnLatch = new CountDownLatch(servers_list.size());

        for (final CacheServiceInterface server : servers_list.values()) {
            server.get(key);
        }
        cntDwnLatch.await();

        // Take the first element
        String rightValue = result_dict.keys().nextElement();

        // Discrepancy in results (either more than one value gotten, or null gotten somewhere)
        if (result_dict.keySet().size() > 1 || result_dict.get(rightValue).size() != servers_list.size()) {

            ArrayList<String> maxValues = maxKeyForTable(result_dict);

            if (maxValues.size() == 1) {

                rightValue = maxValues.get(0);

                ArrayList<String> repairServer = new ArrayList(servers_list.keySet());
                repairServer.removeAll(result_dict.get(rightValue));
                for (String serverUrl : repairServer) {

                    System.out.println("fixing : " + serverUrl + " value: " + rightValue);
                    CacheServiceInterface server = servers_list.get(serverUrl);
                    server.put(key, rightValue);

                }

            } else {

            }
        }

        return rightValue;

    }


    // Returns array of keys with the maximum value
    // If array contains only 1 value, then it is the highest value in the hash map
    public ArrayList<String> maxKeyForTable(ConcurrentHashMap<String, ArrayList<String>> table) {
        ArrayList<String> maxKeys= new ArrayList<String>();
        int maxValue = -1;
        for(Map.Entry<String, ArrayList<String>> entry : table.entrySet()) {
            if(entry.getValue().size() > maxValue) {
                maxKeys.clear(); 
                maxKeys.add(entry.getKey());
                maxValue = entry.getValue().size();
            }
            else if(entry.getValue().size() == maxValue)
            {
                maxKeys.add(entry.getKey());
            }
        }
        return maxKeys;
    }
}