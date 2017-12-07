package edu.duke.distributed_systems;

public class SelectSqlParser {
    //support 3 types of query: SELECT, INSERT, CREATE TABLE
    String query;
    public SelectSqlParser(String q){
        this.query = q;
    }
    // SELECT a,v,c FROM asdf WHERE a = 1;
    public String[] getColumns(){
        int start = 7;
        int end = this.query.indexOf("FROM");
        return this.query.substring(start, end).split(",");
    }




}
