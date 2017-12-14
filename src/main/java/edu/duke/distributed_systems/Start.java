package edu.duke.distributed_systems;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import javax.swing.plaf.metal.MetalTabbedPaneUI;
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
            
            KVClient c = new KVClient(KVStores);
            c.write("abc","abc");
            System.out.println(c.read("abc"));
            
            InsertSqlParser i = new InsertSqlParser("INSERT INTO table_name (column1, column2, column3) VALUES ( value1, value2, value3)");
            System.out.println(i.getColumns());
            System.out.println(i.getTable());
            System.out.println(i.getValues());
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
        SQLLayer layer = new SQLLayer(client, dataStore);
        try {
            layer.processSQL("INSERT INTO Users (id, first, last, email) VALUES (a, b, c, d)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
