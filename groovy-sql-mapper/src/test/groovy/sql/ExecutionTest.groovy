package sql;

import groovy.sql.Sql 
import java.sql.Connection 

import javax.naming.spi.DirStateFactory.Result;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory 
import org.junit.After 
import org.junit.Before 
import org.junit.Test 
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired 
import org.springframework.beans.factory.annotation.Qualifier 
import org.springframework.test.context.ContextConfiguration 
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import sql.model.Schema 

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations=["classpath:applicationContext.xml"])
class ExecutionTest {

    static boolean createData = true
    
    final static Log log = LogFactory.getLog(ExecutionTest.class)
    
    @Autowired
    @Qualifier("dataSource")
    DataSource datasource
    
    QueryBuilder qb
    Schema schema
    Sql sql
    
    @Test
    void aggregateFilter() {
        int index = 0;
        [2012 , 2013, 2014].each { fiscalYear ->
            sql.execute("insert into TEST_SCHEMA.OLD_ITEMS (ITEM_ID, PRICE_AMOUNT, fiscal_yr_id)"
                + " values (${index++}, ${index * 10}, $fiscalYear)")
            sql.execute("insert into TEST_SCHEMA.OLD_ITEMS (ITEM_ID, PRICE_AMOUNT, fiscal_yr_id)"
                + " values (${index++}, ${index * 10}, $fiscalYear)")
        }
        qb.old("fiscalYear+") {
            sum("price")
        }
        def result = qb.execute(sql)
        assert result.size == 3
        assert result[0].price == 30
    }
    
    @Test
    void simpleQuery() {
        qb.items()
        String select = qb.select()
        log.debug(select)
        sql.execute(select)
    }
    
    @Test
    void simpleFilter() {
        qb.items() {
            where(["ITEM_ID", "ITEM_ID_1"])
        }
        def result = qb.execute(sql)
        assert result.size() == 1
        assert result[0]["itemId"] == "ITEM_ID_1"
    }
    
    @Test
    void simpleOrFilter() {
        qb.items("itemId+") {
            where(["itemId", "ITEM_ID_1"]) {
                or(["itemId", "ITEM_ID_2"])
            }
        }
        def result = qb.execute(sql)
        assert result.size() == 2
        assert result[0]["itemId"] == "ITEM_ID_1"
        assert result[1]["itemId"] == "ITEM_ID_2"
    }

    @Test
    void likeFilter() {
        qb.items() {
            where(["state", "like", "%STATE_CD%"])
        }
        def result = qb.execute(sql)
        assert result.size() == 11
    }

    @Test
    void returnMappedInstance() {
        qb.fy()
        def result = qb.execute(sql) { row ->
            return new ComputeYear(fiscalYear : row.fiscalYear)
        }
        assert result.size() == 11
    }
    
    @Test
    void returnInstances() {
        qb.fy()
        def result = qb.execute(sql, ComputeYear.class)
        assert result.size() == 11
        assert result[0] instanceof ComputeYear
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

class ComputeYear {
    def fiscalYear
    def state    
}
