package edu.duke.distributed_systems;

import akka.dispatch.sysmsg.Create;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;

import java.util.List;

public class SQLLayer {
    private KVClient kvClient;
    private MetadataStore metadataStore;
    public SQLLayer(KVClient c){

        this.metadataStore = new MetadataStore();
        this.kvClient = c;
    }
    // This function handles SQL calls
    public String[][] processSQL(String SQL) throws Exception {
        Statement s = CCJSqlParserUtil.parse(SQL);
        if(s instanceof Select){
            return processSelect(SQL);
        }
        else if(s instanceof Insert){
            return processInsert(SQL)? new String[0][0] : null;
        }
        else if(s instanceof CreateTable) {
            return processCreateTable(SQL) ? new String[0][0] : null;
        }

        return new String[0][0];
    }

    private boolean processCreateTable(String SQL) throws Exception {
        return false;
    }

    private String[][] processSelect(String SQL) throws Exception {
        TransactionGenerator TG = new TransactionGenerator(metadataStore);
        Transaction t =  TG.makeSelectTransaction(SQL);
        List<Result> results = this.kvClient.processTransaction(t);
        return null;
    }

    private boolean processInsert(String SQL) throws Exception {
        TransactionGenerator TG = new TransactionGenerator(metadataStore);
        Transaction t =  TG.makeInsertTransaction(SQL);
        this.kvClient.processTransaction(t);
        return true;
    }
}
