package sql

import org.apache.commons.logging.LogFactory;

import org.apache.commons.logging.Log;

import org.junit.Before;
import org.junit.Test 

import static org.junit.Assert.*;

class QueryBuilderFilterTest {

    final static Log log = LogFactory.getLog(QueryBuilderFilterTest.class)
    
    QueryBuilder qb

    @Test 
    void filterAddsRequiredJoin() {
        qb.items() {
            where(["SAU.ACCOUNT_NR", 5])
        }
        def select = qb.select()
        log.debug(select)
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    INNER JOIN TEST_SCHEMA.STORE_AUTH SAU ON
    (
        SAU.ACCOUNT_NR = ITEMS.ACCOUNT_NR AND (SAU.DOC_ID = ITEMS.DOC_ID) AND (SAU.ACCOUNT_NR = 5) AND (SAU.DOC_ID = 'a4')
    )
WHERE SAU.ACCOUNT_NR = ?
"""
        assert select == query
    }
    
    @Test
    void filterWithProjection() {
        qb.items("itemId") {
            where(["state", "1C"]) {
                and(["store", "Abe Lincoln"]) {
                    or(["price", ">", 50])
                }
            }
        }
        def select = qb.select()
        log.debug(select)
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
WHERE ITEMS.STATE_CD = ? AND (ITEMS.STORE_NM = ? OR (ITEMS.PRICE_AMOUNT > ?))
"""
        assert select == query
    }

    @Test
    void filterWithOrderBy() {
        qb.items("itemId+") {
            where(["state", "1C"]) {
                and(["store", "Abe Lincoln"]) {
                    or(["price", ">", 50])
                }
            }
        }
        def select = qb.select()
        log.debug(select)
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
WHERE ITEMS.STATE_CD = ? AND (ITEMS.STORE_NM = ? OR (ITEMS.PRICE_AMOUNT > ?))
ORDER BY ITEMS.ITEM_ID asc
"""
        assert select == query
    }

    
    
    @Test
    void filterOnInvalidColumn() {
        try {
            qb.items() {
                where(["blahBlah", "1C"])
            }
            fail("should not find column blahBlah")
        } catch (Exception e) {
            assert e.message.contains("blahBlah")
        }
    }
        
    @Test
    void nestedFilter() {
        qb.items() {
            where(["state", "1C"]) {
                and(["store", "Abe Lincoln"]) {
                    or(["price", ">", 50])
                }
            }
        }
        def select = qb.select()
        log.debug(select)
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
WHERE ITEMS.STATE_CD = ? AND (ITEMS.STORE_NM = ? OR (ITEMS.PRICE_AMOUNT > ?))
"""
        assert select == query
    }
    
    @Test
    void simpleFilter() {
        qb.items() {
            where(["state", "1C"])
        }
        def select = qb.select()
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
WHERE ITEMS.STATE_CD = ?
"""     
        log.debug(select)
        assert query == select
    }
    
    @Before
    void setup() {
        def mb = new SqlMapBuilder()
        qb = new QueryBuilder(mb.schema("TEST_SCHEMA") {
            
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
        })
    }
        
    
}
