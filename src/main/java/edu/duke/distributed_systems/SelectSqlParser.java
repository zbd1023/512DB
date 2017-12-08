package edu.duke.distributed_systems;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.*;
import java.util.*;
import java.util.List;

public class SelectSqlParser {
    //support 3 types of query: SELECT, INSERT, CREATE TABLE
    PlainSelect p;
    public SelectSqlParser(String q) throws Exception{
        Select select  = (Select) CCJSqlParserUtil.parse(q);
        this.p = (PlainSelect)select.getSelectBody();
    }
    // SELECT a,v,c FROM asdf WHERE a = 1;
    public List<String> getColumns(){
        List<String> res = new ArrayList<>();
        List<SelectItem> sl  = p.getSelectItems();
        if(sl.get(0) instanceof AllColumns){
            res.add("*");
        }
        else{
            this.p.getSelectItems().forEach(a ->res.add(((Column)(((SelectExpressionItem )a).getExpression())).getColumnName()));
        }
        return res;
    }
    public String getTable(){
        return ((Table)(this.p.getFromItem())).getName();
    }
    public String getWhere(){
        if(this.p.getWhere() != null){
            List<String> res = new ArrayList<>();
            return this.p.getWhere().toString();
        }
        else
            return "";

    }





}
