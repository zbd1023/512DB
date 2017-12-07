package edu.duke.distributed_systems;
import akka.actor.ActorRef;
import scala.concurrent.*;
import scala.concurrent.duration.*;
import akka.util.*;
import akka.pattern.*;


public class KVClient {
    private ActorRef[] KVStores;
    public KVClient(ActorRef[] KVS){
        this.KVStores = KVS;
    }

    public String read(String key) throws Exception{
        Timeout timeout = new Timeout(Duration.create(1, "seconds"));
        Future<Object> future = Patterns.ask(route(key), new KVStore.get(key), timeout);
        return (String) Await.result(future, timeout.duration());
    }

    public void write(String key, String value) throws Exception{
        Timeout timeout = new Timeout(Duration.create(1, "seconds"));
        Future<Object> future = Patterns.ask(route(key), new KVStore.put(key, value), timeout);
        Await.result(future, timeout.duration());
    }

    private ActorRef route(String key){
        // need to figure out this routing function
        return KVStores[0];
    }

}
