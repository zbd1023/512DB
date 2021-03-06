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
    public SQLLayer(KVClient c, MetadataStore store) {
        this.metadataStore = store;
        this.kvClient = c;
    }
    // This function handles SQL calls
    public Result processSQL(String SQL) throws Exception {
        Statement s = CCJSqlParserUtil.parse(SQL);
        if(s instanceof Select){
            return processSelect(SQL);
        }
        else if(s instanceof Insert){
            return processInsert(SQL)? new AggregateResults() : null;
        }
        else if(s instanceof CreateTable) {
            return processCreateTable(SQL) ? new AggregateResults() : null;
        }

        return new AggregateResults();
    }

    private boolean processCreateTable(String SQL) throws Exception {
        return false;
    }

    private Result processSelect(String SQL) throws Exception {
        TransactionGenerator TG = new TransactionGenerator(metadataStore);
        Transaction t =  TG.makeSelectTransaction(SQL);
        AggregateResults ag = (AggregateResults)this.kvClient.processTransaction(t);
        ag.applyWhere(new SelectSqlParser(SQL).getWhere());
        return ag;
    }

    private boolean processInsert(String SQL) throws Exception {
        TransactionGenerator TG = new TransactionGenerator(metadataStore);
        Transaction t =  TG.makeInsertTransaction(SQL);
        this.kvClient.processTransaction(t);
        return true;
    }
}
