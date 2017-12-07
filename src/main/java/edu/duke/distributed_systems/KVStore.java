package edu.duke.distributed_systems;
import akka.actor.AbstractActor;
import akka.actor.Props;

import java.security.Key;
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

    public static class scan {
        public String start;
        public String end;
        public scan(String s, String e){
            this.start = s;
            this.end = e;
        }
    }

    private TreeMap<String, String> store = new TreeMap<>();
    // Assume a distributed, strongly-consistent, transactional key-value store


    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .match(put.class, p -> Put(p.key, p.value))
                .match(get.class, g -> Get(g.key))
                .match(scan.class, s -> Scan(s.start, s.end))
                .build();
    }

    private void Put(String k, String v){
        getSender().tell(store.put(k, v), getSelf());
    }
    private void Get(String k){
//        SortedMap<String, String> scan = store.subMap("", "");
//        scan.
        getSender().tell(store.get(k), getSelf());
    }
    private void Scan(String s, String e){
        getSender().tell(store.subMap(s, e), getSelf());
    }
}
