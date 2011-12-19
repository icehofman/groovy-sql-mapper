package sql.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings({"serial", "rawtypes"})
public class CriteriaBase implements Serializable {

    protected String andOr;
    protected String op = "=";
    protected ColumnBase lhs;
    protected ColumnBase rhs;
    protected CriteriaBase parent;
    protected String leftColumn;
    protected String rightColumn;
    protected Set joinAlias = new HashSet();
    protected List criteria = new ArrayList();

    public CriteriaBase() {
        super();
    }

    public CriteriaBase(String leftColumn, String rightColumn) {
        super();
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    public CriteriaBase(ColumnBase lhs, ColumnBase rhs) {
        super();
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public String getAndOr() {
        return andOr;
    }

    public void setAndOr(String andOr) {
        this.andOr = andOr;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public ColumnBase getLhs() {
        return lhs;
    }

    public void setLhs(ColumnBase lhs) {
        this.lhs = lhs;
    }

    public ColumnBase getRhs() {
        return rhs;
    }

    public void setRhs(ColumnBase rhs) {
        this.rhs = rhs;
    }

    public CriteriaBase getParent() {
        return parent;
    }

    public void setParent(CriteriaBase parent) {
        this.parent = parent;
    }

    public String getLeftColumn() {
        return leftColumn;
    }

    public void setLeftColumn(String leftColumn) {
        this.leftColumn = leftColumn;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    public void setRightColumn(String rightColumn) {
        this.rightColumn = rightColumn;
    }

    public Set getJoinAlias() {
        return joinAlias;
    }

    public void setJoinAlias(Set joinAlias) {
        this.joinAlias = joinAlias;
    }

    public List getCriteria() {
        return criteria;
    }

    public void setCriteria(List criteria) {
        this.criteria = criteria;
    }
    
    public String indent(int x) {
        String indent = "";
        for (int i = 0; i < x; i++) {
            indent += "    ";
        }
        return indent;
    }
    
}
