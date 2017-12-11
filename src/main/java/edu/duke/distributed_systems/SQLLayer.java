package edu.duke.distributed_systems;

import akka.actor.ActorRef;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;

public class SQLLayer {
    private KVClient kvClient;
    public SQLLayer(KVClient c){
        this.kvClient = c;
    }
    // This function handles SQL calls
    public String[][] processSQL(String SQL) throws Exception{
        Statement s = CCJSqlParserUtil.parse(SQL);
        if(s instanceof Select){
            return processSelect(SQL);
        }
        else if(s instanceof Insert){
            return processInsert(SQL)? new String[0][0] : null;
        }
        return new String[0][0];
    }

    private String[][] processSelect(String SQL) throws Exception{
        TransactionGenerator TG = new TransactionGenerator(new MetadataStore());
        Transaction t =  TG.makeSelectTransaction(SQL);
        this.kvClient.processTransaction(t);
        // need to filter where
        return null;
    }

    private boolean processInsert(String SQL) throws Exception{
        TransactionGenerator TG = new TransactionGenerator(new MetadataStore());
        Transaction t =  TG.makeInsertTransaction(SQL);
        this.kvClient.processTransaction(t);
        return true;
    }
}
