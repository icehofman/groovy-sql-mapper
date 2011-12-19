package sql.model;

@SuppressWarnings("serial")
public class ColumnBase extends SqlModelBase {

    TableBase table;
    String aggFunction;
    Boolean literal;
    Boolean agg;

    public ColumnBase() {
        super();
    }

    public ColumnBase(String alias, String name) {
        super(alias, name);
    }

    public TableBase getTable() {
        return table;
    }

    public void setTable(TableBase table) {
        this.table = table;
    }

    public String getAggFunction() {
        return aggFunction;
    }

    public void setAggFunction(String aggFunction) {
        this.aggFunction = aggFunction;
    }

    public Boolean getLiteral() {
        return literal;
    }

    public void setLiteral(Boolean literal) {
        this.literal = literal;
    }

    public Boolean getAgg() {
        return agg;
    }

    public void setAgg(Boolean agg) {
        this.agg = agg;
    }

}
