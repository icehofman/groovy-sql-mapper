package sql;
import org.apache.commons.logging.Log 
import org.apache.commons.logging.LogFactory 
import org.junit.Before 
import org.junit.Test 

import static org.junit.Assert.*

class QueryBuilderTest {

    final Log log = LogFactory.getLog(QueryBuilderTest.class)

    QueryBuilder qb
    
    @Test
    void literalAggregate() {
        qb.DIM_A(["docId", "maxAccountNr"])
        String select = qb.select()
        log.debug(select)
        def query = """
SELECT
    DIM_A.DOC_ID as "docId",
    DIM_A.MAX(ACCOUNT_NR) as "maxAccountNr"
FROM TEST_SCHEMA.DIM_A DIM_A
GROUP BY DIM_A.DOC_ID
"""
        assert query == select
    }
    
    @Test
    void multiTableJoinCriteria() {
        qb.di("si.state")
        String select = qb.select()
        log.debug(select)
        def query = """
SELECT
    SI.STATE_CD as "state"
FROM TEST_SCHEMA.ITEMS_DUE_IN DI
    LEFT OUTER JOIN TEST_SCHEMA.STORE_ITEM SI ON
    (
        SI.ITEM_CD = DI.ITEM_CD OR (SI.STORE_NM = DI.STORE_NM AND (SI.STATE_CD = FY.STATE_CD))
    )
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
"""
       assert query == select 
    }
    
    @Test
    void literalNoAlias() {
        try {
            qb.ITEMS() {
                sum("coalesce(FY.FISCAL_YR_ID, 0)")
                join("FY")
            }
            fail("literal aggregate requires an alias")
        } catch (Exception e) {
            log.debug(e.getMessage())
        }
    }   
    
     
    @Test
    void orderLiteral() {
        qb.ITEMS() {
            sum("coalesce(FY.FISCAL_YR_ID, 0)+", alias: "fiscalYearCoalesce")
            join("FY")
        }
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId",
    sum(coalesce(FY.FISCAL_YR_ID, 0)) as "fiscalYearCoalesce"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
GROUP BY ITEMS.ITEM_ID, ITEMS.PRICE_AMOUNT, ITEMS.ITEM_CD, ITEMS.STATE_CD, ITEMS.STORE_NM, ITEMS.ACCOUNT_NR, ITEMS.DOC_ID
ORDER BY sum(coalesce(FY.FISCAL_YR_ID, 0)) asc
"""
        def select = qb.select()
        log.debug(select)
        assert query == select
    }

    
    @Test
    void aggregateOrdering() {
        qb.ITEMS("store") {
            count("fy.fiscalYear-")
        }
        def query = """
SELECT
    ITEMS.STORE_NM as "store",
    count(FY.FISCAL_YR_ID) as "fiscalYear"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
GROUP BY ITEMS.STORE_NM
ORDER BY count(FY.FISCAL_YR_ID) desc
"""
        def select = qb.select()
        log.debug(select)
        assert query == select
    }
    
    @Test
    void aggregateLiteral() {
        qb.ITEMS() {
            sum("coalesce(FY.FISCAL_YR_ID, 0)", alias: "fiscalYearCoalesce")
            join("FY")
        }
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId",
    sum(coalesce(FY.FISCAL_YR_ID, 0)) as "fiscalYearCoalesce"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
GROUP BY ITEMS.ITEM_ID, ITEMS.PRICE_AMOUNT, ITEMS.ITEM_CD, ITEMS.STATE_CD, ITEMS.STORE_NM, ITEMS.ACCOUNT_NR, ITEMS.DOC_ID
"""
        def select = qb.select()
        log.debug(select)
        assert query == select
    }
    
    @Test
    void aggregateReAlias() {
        qb.ITEMS() {
            sum("FY.fiscalYear", alias: "fiscalYearTotal")
        }
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId",
    sum(FY.FISCAL_YR_ID) as "fiscalYearTotal"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
GROUP BY ITEMS.ITEM_ID, ITEMS.PRICE_AMOUNT, ITEMS.ITEM_CD, ITEMS.STATE_CD, ITEMS.STORE_NM, ITEMS.ACCOUNT_NR, ITEMS.DOC_ID
"""
        def select = qb.select()
        log.debug(select)
        assert query == select
    }
    
    @Test
    void aggregateOnPlural() {
        qb.ITEMS() {
            sum("FY.fiscalYear")
        }
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId",
    sum(FY.FISCAL_YR_ID) as "fiscalYear"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
GROUP BY ITEMS.ITEM_ID, ITEMS.PRICE_AMOUNT, ITEMS.ITEM_CD, ITEMS.STATE_CD, ITEMS.STORE_NM, ITEMS.ACCOUNT_NR, ITEMS.DOC_ID
"""
        def select = qb.select()
        log.debug(select)
        assert query == select
    }

    @Test
    void allColumnsAggregate() {
        qb.ITEMS() {
            sum("fy.fiscalYear")
        }
        def query = """
SELECT
    ITEMS.ITEM_ID as "itemId",
    ITEMS.PRICE_AMOUNT as "price",
    ITEMS.ITEM_CD as "itemCode",
    ITEMS.STATE_CD as "state",
    ITEMS.STORE_NM as "store",
    ITEMS.ACCOUNT_NR as "account",
    ITEMS.DOC_ID as "docId",
    sum(FY.FISCAL_YR_ID) as "fiscalYear"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    CROSS JOIN TEST_SCHEMA.FISCAL_YR FY
GROUP BY ITEMS.ITEM_ID, ITEMS.PRICE_AMOUNT, ITEMS.ITEM_CD, ITEMS.STATE_CD, ITEMS.STORE_NM, ITEMS.ACCOUNT_NR, ITEMS.DOC_ID
"""
        def select = qb.select()
        log.debug(select)
        assert query == select
    }
    
    @Test
    void aggregateOnSingularFailure() {
        try {
            qb.ITEMS() {
                sum("SAU.docId")
            }
            fail("cannot aggregate a singular attribute")
        } catch (Exception e) {
            log.debug(e.getMessage())
        }
    }
    
    
    @Test
    void literalTest() {
        qb.ITEMS("SAU.docId")
        def select = qb.select()
        log.debug(select)
        def query = """
SELECT
    SAU.DOC_ID as "docId"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    INNER JOIN TEST_SCHEMA.STORE_AUTH SAU ON
    (
        SAU.ACCOUNT_NR = ITEMS.ACCOUNT_NR AND (SAU.DOC_ID = ITEMS.DOC_ID) AND (SAU.ACCOUNT_NR = 5) AND (SAU.DOC_ID = 'a4')
    )
"""
        assert select == query
    }
    
    @Test
    void basicJoin() {
        qb.ITEMS("si.state")
        def select = qb.select()
        log.debug(select)
        def query = """
SELECT
    SI.STATE_CD as "state"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    LEFT OUTER JOIN TEST_SCHEMA.STORE_ITEM SI ON
    (
        SI.ITEM_CD = ITEMS.ITEM_CD OR (SI.STORE_NM = ITEMS.STORE_NM AND (SI.STATE_CD = ITEMS.STATE_CD))
    )
"""
        assert select == query
    }
    
    @Test
    void columnNotFoundError() {
        try {
            qb.ITEMS("blahBlah")
            fail("should not find column")
        } catch (Exception e) { 
            assert e.message.contains("blahBlah")
        }
    }
    
    @Test
    void orderDescending() {
        qb.ITEMS(["itemCode-", "si.state"]) {
            sum("price")
        }
        def select = qb.select()

        def query = """
SELECT
    ITEMS.ITEM_CD as "itemCode",
    SI.STATE_CD as "state",
    sum(ITEMS.PRICE_AMOUNT) as "price"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
    LEFT OUTER JOIN TEST_SCHEMA.STORE_ITEM SI ON
    (
        SI.ITEM_CD = ITEMS.ITEM_CD OR (SI.STORE_NM = ITEMS.STORE_NM AND (SI.STATE_CD = ITEMS.STATE_CD))
    )
GROUP BY ITEMS.ITEM_CD, SI.STATE_CD
ORDER BY ITEMS.ITEM_CD desc
"""
        log.debug(select)
        assert select == query
    }

    @Test
    void queryAggregate() {
        qb.ITEMS("itemCode") {
            sum("price")
        }
        def select = qb.select()
        
        def query = """
SELECT
    ITEMS.ITEM_CD as "itemCode",
    sum(ITEMS.PRICE_AMOUNT) as "price"
FROM TEST_SCHEMA.ITEM_DTL ITEMS
GROUP BY ITEMS.ITEM_CD
"""
        assert query == select
    }
    
    @Test
    void queryTableNotFound() {
        try {
            qb.blahBlah()
            fail("should throw exception because table is not in schema")
        } catch (Exception e) {
            log.debug(e.getMessage())
        }
    }
    
    @Test
    void queryTableAlias() {
        qb.ITEMS()
        assert qb.root.name == "ITEM_DTL"
        assert qb.root.alias == "ITEMS"
    }
    
    @Test
    void queryTableName() {
        def tableName = "ITEM_DTL"
        qb."$tableName"()
        assert qb.root.name == "ITEM_DTL"
        assert qb.root.alias == "ITEMS"
    }
    
    @Test
    void singleTableSelect() {
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
"""
        qb.ITEMS()
        def select = qb.select()
        log.debug(select)
        assert select == query
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
