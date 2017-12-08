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
            SelectSqlParser s = new SelectSqlParser("select a,b,c from asdf where a>c or b=c");
            System.out.println(s.getColumns());
            System.out.println(s.getTable());
            System.out.println(s.getWhere());
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ioe) {
        } finally {
            system.terminate();
        }

    }
}

