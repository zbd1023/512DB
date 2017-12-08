package edu.duke.distributed_systems;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.*;
import net.sf.jsqlparser.expression.StringValue;
import java.util.*;

public class InsertSqlParser {
    Insert i;
    public InsertSqlParser(String q) throws Exception{
        this.i  = (Insert) CCJSqlParserUtil.parse(q);
    }
//    public HashMap<String, String> getValuePair(){
//
//    }

    public String getTable(){
        return this.i.getTable().getName();
    }

    public List<String> getColumns(){
        List<String> res = new ArrayList<>();
        this.i.getColumns().forEach(s -> res.add(s.getColumnName()));
        return res;
    }

    public List<String> getValues(){
        List<String> res = new ArrayList<>();
        ((ExpressionList)this.i.getItemsList()).getExpressions().forEach( s -> res.add(((Column)s).getColumnName()));
        return res;
    }
}
