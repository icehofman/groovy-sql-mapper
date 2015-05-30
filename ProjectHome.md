The project focus is on supporting optimized and efficient query generation with a minimal amount of developer effort.  Project was originally designed with data warehouse reporting in mind, and supports flexible join criteria.

Two DSL "builders" are provided.  One for describing database schemas, and the other for working with schemas to create SQL dynamically.


Here's an example of defining a Schema:

```

def schema = new SqlMapBuilder().schema(name: 'schemaName', alias: 's1') {
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

```


Once you have a Schema defined using an SqlMapBuilder, you can use it to generate SQL with a QueryBuilder.  Using the previous code, we can add to it with this:

```
  QueryBuilder qb = new QueryBuilder(schema)
  qb.table1()
  System.out << qb.select()
```

Results in:

```
SELECT
    t1.column1 as "c1",
    t1.column2 as "c2",
    t1.column3 as "c3"
FROM schemaName.table1 t1
```

Here's a more complicated example:

```

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
        return qb.execute(sql) // groovy.sql.Sql instance
    }
```

Have a look at the [tests](http://code.google.com/p/groovy-sql-mapper/source/browse/#git%2Fgroovy-sql-mapper%2Fsrc%2Ftest%2Fgroovy%2Fsql) for more complicated usage examples, including joins, criteria, literals, aggregate functions, and more.