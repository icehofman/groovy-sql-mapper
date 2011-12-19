package sql

import javax.sql.DataSource

import groovy.sql.Sql 

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory 
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier 
import org.springframework.test.context.ContextConfiguration 
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

import requests.ReportRequest 
import sql.model.QueryDefinitionException 
import sql.model.Schema 

import static org.junit.Assert.*

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations=["classpath:applicationContext.xml"])
class RequestTest {
    
    static boolean createData = true
    
    final static Log log = LogFactory.getLog(RequestTest.class)
    
    @Autowired
    @Qualifier("dataSource")
    DataSource datasource
    
    QueryBuilder qb
    Schema schema
    Sql sql

    @Test
    void queryGenerationFromRequestDetails() {
        ReportRequest request = new ReportRequest(tableName: "items", filters : ["itemId" : ["ITEM_ID_1"]])
        List<Map> results = doRequest(request)
        assert results.size() == 1
    }

        
    @Test
    void queryGenerationFromRequestMetrics() {
        ReportRequest request = new ReportRequest(tableName: "items", columns : ["itemId"],  
            filters : ["itemId" : ["ITEM_ID_1", "ITEM_ID_2"], "itemCode" : ["ITEM_CD_1", "ITEM_CD_2"]], 
            sums : ["price" : "price"], counts : ["price" : "quantity"])
        List<Map> results = doRequest(request)
        assert results.size() == 2
    }
    
    List<Map> doRequest(ReportRequest request) {
        qb."${request.tableName}"(request.columns) {
            request.sums.each { column, alias ->
                sum(column, "alias" : alias)
            }
            request.counts.each { column, alias ->
                count(column, "alias" : alias)
            }
            where() {
                request.filters.each { alias, values ->
                    if (values) {
                        and ([alias, values[0]]) {
                            if (values[1]) {
                                values[1..-1].each { value ->
                                    or([alias, value])
                                }
                            }
                        }
                    }
                }
            }
        }
        return qb.execute(sql)
    }
    
    @Before
    void setup() throws Exception {
        schema = new SqlMapBuilder().schema("TEST_SCHEMA") {
            
            table(name: "DIM_A", alias: "DIM_A") {
                column(name: "DOC_ID", alias: "docId")
                column(name: "MAX(ACCOUNT_NR)", alias: "maxAccountNr", agg: true)
            }
            
            table(name: "ITEM_DTL", alias: "ITEMS") {
                column(name: "ITEM_ID", alias: "itemId")
                column(name: "PRICE_AMOUNT", alias: "price")
                column(name: "ITEM_CD", alias: "itemCode")
                column(name: "STATE_CD", alias: "state")
                column(name: "STORE_NM", alias: "store")
                column(name: "ACCOUNT_NR", alias: "account")
                column(name: "DOC_ID", alias: "docId")
                singular(name: "STORE_ITEM", alias: "si", left: true) {
                    on("ITEM_CD") {
                        or("store") {
                            and("STATE_CD")
                        }
                    }
                }
                singular(name: "STORE_AUTH") {
                    on("account", "ACCOUNT_NR") {
                        and("DOC_ID")
                        and("account", 5)
                        and("DOC_ID", "'a4'")
                    }
                }
                plural(name: "FISCAL_YR", cross: true)
            }
            
            table(name: "OLD_ITEMS", alias: "OLD") {
                column(name: "ITEM_ID", alias: "itemId")
                column(name: "PRICE_AMOUNT", alias: "price")
                column(name: "FISCAL_YR_ID", alias: "fiscalYear")
                plural(name: "FISCAL_YR") {
                    on("FISCAL_YR_ID", ">", "FISCAL_YR_ID") {
                        or("FISCAL_YR_ID", "2011")
                    }
                }
            }
           
            table(name: "STORE_ITEM", alias: "SI") {
                column(name: "ITEM_CD", alias: "itemCode")
                column(name: "STORE_NM", alias: "store")
                column(name: "STATE_CD", alias: "state")
                plural(name: "ITEM_DTL") {
                    on("ITEM_CD")
                }
            }
            
            table(name: "STORE_AUTH", alias: "SAU") {
                column(name: "ACCOUNT_NR", alias: "account")
                column(name: "DOC_ID", alias: "docId")
            }
            
            table(name: "FISCAL_YR", alias: "FY") {
                column(name: "FISCAL_YR_ID", alias: "fiscalYear")
                column(name: "STATE_CD", alias: "state" )
            }

            
            table(name: "ITEMS_DUE_IN", alias: "DI") {
                column(name: "ITEM_ID", alias: "itemId")
                column(name: "ITEM_CD", alias: "itemCode")
                column(name: "STORE_NM", alias: "store")
                plural(name: "FISCAL_YR", alias: "FY", cross: true)
                singular(name: "STORE_ITEM", left: true) {
                    on("ITEM_CD") {
                        or("store") {
                            and("STATE_CD", "FY.STATE_CD")
                        }
                    }
                }
            }
        }
        qb = new QueryBuilder(schema)

        sql = new Sql(datasource);
        if (createData) {
            try {
                sql.execute("drop schema ${schema.name}".toString())
            } catch (Exception ex) {
                // not a problem
            }
            
            schema.generateCreateScript(["price" : "int(8)"]).each { query ->
                    log.debug(query)
                    sql.execute(query.toString())
            }
            
            def generators = [:]
            generators["price"] = { index ->
                return index * 1000
            }
            
            def executor = {query ->
                log.debug(query)
                sql.execute(query.toString())
            }
            
            ["items", "fy"].each { alias ->
                schema.generateTestData(alias, generators).each(executor)
            }
            
            createData = false
        }
    }
    
}