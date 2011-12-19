package sql.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
public class SchemaBase extends SqlModelBase implements Serializable {

    List tables = new ArrayList();

    public SchemaBase() {
        super();
    }

    public SchemaBase(String name) {
        super(name, name);
    }

    public SchemaBase(String alias, String name) {
        super(alias, name);
    }

    public List getTables() {
        return tables;
    }

    public void setTables(List tables) {
        this.tables = tables;
    }

    public void addTable(TableBase table) {
        tables.add(table);
    }

}
