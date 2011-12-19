package sql.model

class Table extends TableBase {

    public Table() {
        super();
    }

    public Table(String alias, String name) {
        super(alias, name);
    }

    JoinBase findJoin(String alias) {
        return (singulars + plurals).find { it && it.alias && it.alias.equalsIgnoreCase(alias) }
    }

    List findColumns(String colName) {
        def column = findColumn(colName)["column"]
        if (column) {
            return [column]
        } else {
            return []
        }
    }

    Map findColumn(String colName) {
        def split = colName.tokenize(".")
        if (split.size() == 2) {
            def join = findJoin(split[0])
            if (join) {
                if (!join.table) {
                    join.table = schema.findTable(split[0])
                }
                return ["join" : join, "column" : join.table.findColumn(split[1])["column"]]
            }
        } else {
            String name = colName.replaceAll("\\+|\\-", "")
            Column column = columns.find { it.alias ==  name }

            if (!column) {
                column = columns.find { it.name == name }
                if (!column) {
                    throw new ColumnNotFoundException("Column '$colName' not found in table '${this.name}'")
                }
            }

            return ["column": column.clone()]
        }
    }

    String toSqlMapDSL() {
        String dsl = "${indent(1)}table(name: '$name', alias: '$alias') {\n"
        columns.each { column -> dsl += "${indent(2)}column(name: '${column.name}', alias: '${column.alias}')\n" }
        (singulars + plurals).each { tableJoin ->  dsl += "${tableJoin.toSqlMapDSL()}" }
        return dsl += "${indent(1)}}\n"
    }

    List findColumns(List colNames) {
        return colNames.collect { findColumn(it)["column"] }
    }

    String reference() {
        return "${id()} $alias"
    }

    String id() {
        return "${schema.name}.${name}"
    }
}
