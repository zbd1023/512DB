package edu.duke.distributed_systems;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.*;

public class KVStore extends AbstractActor{
    static public Props props() {
        return Props.create(KVStore.class);
    }
    public static class put {
        public String key;
        public String value;
        public put(String k, String v){
            this.key = k;
            this.value = v;
        }
    }
    public static class get {
        public String key;
        public get(String k){
            this.key = k;
        }
    }

    private HashMap<String, String> store = new HashMap<>();
    // Assume a sharded key-value store.


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(put.class, p -> Put(p.key, p.value))
                .match(get.class, g -> Get(g.key))
                .build();
    }

    private void Put(String k, String v){
        getSender().tell(store.put(k, v), getSelf());
    }
    private void Get(String k){
        getSender().tell(store.get(k), getSelf());
    }
}
