package sql.model

class Schema extends SchemaBase {

    public Schema() {
        super();
    }

    public Schema(String alias, String name) {
        super(alias, name);
    }

    public Schema(String name) {
        super(name);
    }

    Table findTable(String name) {
        return tables.find { it.alias.equalsIgnoreCase(name) || it.name.equalsIgnoreCase(name) }
    }

    List generateTestData(String alias, Map generators) {
        List inserts = []
        Table table = findTable(alias)
        for (def index : 0..10) {
            String insert = "insert into ${table.id()} ("
            insert += table.columns.collect { it.name }.join(", ")
            insert += " ) values ( "
            insert += table.columns.collect {
                def generator = generators[it.alias]
                if (generator) {
                    return generator(index)
                } else {
                    return "'${it.name}_$index'"
                }
            }.join(", ")
            insert += ")"
            inserts += insert
        }
        return inserts
    }

    List generateCreateScript(Map typeMap = [:], String dOpen = '"', String dClose = '"') {
        List lines = []
        lines += "create schema $name"
        lines.addAll(tables.collect { table ->
            def line = "\ncreate table ${dOpen + name + dClose}.${dOpen + table.name + dClose} (\n"
            line += table.columns.collect { column -> "    $dOpen$column.name$dClose ${typeMap[column.alias]?:'varchar(255)'}" }.join(",\n")
            line += "\n)\n"
        })
        return lines
    }

    String toSqlMapDSL() {
        String dsl = "schema(name: '$name', alias: '$alias') {\n"
        tables.each { table ->
            dsl += table.toSqlMapDSL()
        }
        return dsl + "}\n"
    }
}
