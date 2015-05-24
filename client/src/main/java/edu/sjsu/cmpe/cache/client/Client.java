package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Unirest;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        ClientCRDT objNewClient = new ClientCRDT();
      
        boolean checkResult = objNewClient.put(1, "a");
        System.out.println("result : " + checkResult);
        Thread.sleep(30*1000);
        System.out.println("Step 1: put(1 > a) ----> sleep for 30s");


        objNewClient.put(1, "b");
        Thread.sleep(30*1000);
        System.out.println("Step 2: put(1 > b) ----> sleep for 30s");


        String value = objNewClient.get(1);
        System.out.println("Step 3: get(1) ----> " + value);

        System.out.println("Exiting Client...");
        Unirest.shutdown();
    }

}
