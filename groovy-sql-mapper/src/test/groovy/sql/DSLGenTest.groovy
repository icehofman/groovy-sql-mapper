package sql

import org.junit.Before
import org.junit.Test

import sql.model.Schema

class DSLGenTest {

    @Test
    public void genDSL() {
        System.out.println(schemaDef)
        System.out.println(schema.toSqlMapDSL())
        assert schema.toSqlMapDSL() == schemaDef
    }
	
	@Test
	public void select() {
		QueryBuilder qb = new QueryBuilder(schema)
		qb.table1()
		System.out << qb.select()
	}
    
    Schema schema
    String schemaDef = 
$/schema(name: 'schemaName', alias: 's1') {
    table(name: 'table1', alias: 't1') {
        column(name: 'column1', alias: 'c1')
        column(name: 'column2', alias: 'c2')
        column(name: 'column3', alias: 'c3')
        plural(name: 'table2', alias: 't1tot2', inner: true) {
            on(['c1', '=', 'c2'])
        }
    }
    table(name: 'table2', alias: 't2') {
        column(name: 'column1', alias: 'c1')
        column(name: 'column2', alias: 'c2')
        column(name: 'column3', alias: 'c3')
        singular(name: 'table1', alias: 't2tot1', inner: true) {
            on(['c3', '=', 'c3']) {
                and(['c2', '=', 'c2']) {
                    or(['c1', '=', 'c1'])
                }
            }
        }
    }
}
/$
    
    @Before
    public void setup() {
        SqlMapBuilder builder = new SqlMapBuilder();
        Eval.me("builder", builder, "builder." + schemaDef)
        schema = builder.root
    }
    
}
