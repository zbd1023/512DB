package edu.duke.distributed_systems;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.io.IOException;
import java.util.*;

public class Start {
    final static int num = 10;

    public static void main(String[] args) throws Exception {
        final ActorSystem system = ActorSystem.create("512DB");

        try {
            ActorRef[] KVStores = new ActorRef[num];
            for(int i =0; i < KVStores.length; i++){
                KVStores[i] = system.actorOf(KVStore.props(), "KVStore" + i);
            }
            testQuery(KVStores);
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ioe) {
        	ioe.printStackTrace();
        } finally {
            system.terminate();
        }
    }


    private static void testQuery(ActorRef stores[]) {
        KVClient client = new KVClient(stores);
        MetadataStore dataStore = new MetadataStore();
        dataStore.setPrimaryKey("Users", "id");
        SQLLayer layer = new SQLLayer(client, dataStore);
        try {
            layer.processSQL("INSERT INTO Users (id, first, last, email) VALUES (bz43, Wilson, Zhang, edu)");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            layer.processSQL("INSERT INTO Users (id, first, last, email) VALUES (bz44, Jane, Doe, edu)");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            layer.processSQL("INSERT INTO Users (id, first, last, email) VALUES (bz45, Jane, Dan, edu)");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println(layer.processSQL("SELECT id, first, last, email FROM Users where first = Jane").getResult());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
