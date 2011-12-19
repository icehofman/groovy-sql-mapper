package sql.model;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
public class TableBase extends SqlModelBase {

    List<ColumnBase> columns = new ArrayList<ColumnBase>();
    List singulars = new ArrayList();
    List plurals = new ArrayList();

    SchemaBase schema;

    public TableBase() {
        super();
    }

    public TableBase(String alias, String name) {
        super(alias, name);
    }

    public List<ColumnBase> getColumns() {
        return columns;
    }

    public void setColumns(List columns) {
        this.columns = columns;
    }

    public SchemaBase getSchema() {
        return schema;
    }

    public void setSchema(SchemaBase schema) {
        this.schema = schema;
    }

    public ColumnBase findColumnByName(String name) {
        for (ColumnBase column : columns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    public void addJoin(JoinBase join, boolean singular) {
        if (singular && !singulars.contains(join)) {
            singulars.add(join);
        } else if (!plurals.contains(join)) {
            plurals.add(join);
        }
    }

    public List getSingulars() {
        return singulars;
    }

    public void setSingulars(List singulars) {
        this.singulars = singulars;
    }

    public List getPlurals() {
        return plurals;
    }

    public void setPlurals(List plurals) {
        this.plurals = plurals;
    }

}
