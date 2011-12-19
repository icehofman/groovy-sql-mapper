package sql.model;

@SuppressWarnings("serial")
public class JoinBase extends SqlModelBase {

    boolean left;
    boolean cross;
    boolean inner = true;
    TableBase target;
    TableBase table;
    CriteriaBase criteria;

    public JoinBase() {
        super();
    }

    public JoinBase(String alias, String name) {
        super(alias, name);
    }

    public boolean isLeft() {
        return left;
    }

    public boolean isCross() {
        return cross;
    }

    public boolean isInner() {
        return inner;
    }

    public void setInner(boolean inner) {
        this.inner = inner;
    }

    public TableBase getTarget() {
        return target;
    }

    public void setTarget(TableBase target) {
        this.target = target;
    }

    public TableBase getTable() {
        return table;
    }

    public void setTable(TableBase table) {
        this.table = table;
    }

    public CriteriaBase getCriteria() {
        return criteria;
    }

    public void setCriteria(CriteriaBase criteria) {
        this.criteria = criteria;
    }

    public void setLeft(boolean left) {
        this.left = left;
        if (left) {
            inner = false;
        }
    }

    public void setCross(boolean cross) {
        this.cross = cross;
        if (cross) {
            inner = false;
        }
    }

}
