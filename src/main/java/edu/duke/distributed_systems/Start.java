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

//            KVClient c = new KVClient(KVStores);
//            MetadataStore metaStore = new MetadataStore();
//            metaStore.setPrimaryKey("tb", "column1");
//            SQLLayer sqlLayer = new SQLLayer(c,metaStore);
//            sqlLayer.processSQL("INSERT INTO tb (column1, column2, column3) VALUES ( key1, value2, value3)");
//            sqlLayer.processSQL("INSERT INTO tb (column1, column2, column3) VALUES ( key2, value4, value5)");
//            System.out.println(sqlLayer.processSQL("SELECT column1, column2 FROM tb").getResult());
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
            layer.processSQL("INSERT INTO Users (id, first, last, email) VALUES (asf, Wilson, Zhang, bzduke.edu)");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            layer.processSQL("INSERT INTO Users (id, first, last, email) VALUES (asfg, Jane, Doe, jdduke.edu)");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println(layer.processSQL("SELECT id, first, last, email FROM Users").getResult());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
