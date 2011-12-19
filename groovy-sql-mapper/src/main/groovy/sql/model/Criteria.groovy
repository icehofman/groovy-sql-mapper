package sql.model

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory


class Criteria extends CriteriaBase {
    
    private final static Log log = LogFactory.getLog(Criteria.class)
    
    JoinBase critJoin
    JoinBase additionalJoin
    
    public Criteria(String joinColumn) {
        super();
        leftColumn = rightColumn = joinColumn
    }
    
    public Criteria(String leftColumn, String rightColumn) {
        super();
        this.leftColumn = leftColumn
        this.rightColumn = rightColumn
    }

    
    public Criteria(List definition) {
        if (!definition) {
            return;
        }
        switch (definition.size()) {
            case 1:
                leftColumn = rightColumn = definition[0]
                break;
            case 2:
                leftColumn = definition[0]
                rightColumn = definition[1]
                break;
            case 3:
                leftColumn = definition[0]
                op = definition[1]
                rightColumn = definition[2]
                break;
            default:
                throw new RuntimeException("Incorrect criteria definition")
        }
    }
    
    public Criteria(Column lhs, Column rhs) {
        this.lhs = lhs
        this.rhs = rhs
    }
    
    public Criteria(Column lhs, Column rhs, String op) {
        this(lhs, rhs)
        this.op = op
    }
    
    public Criteria(Column lhs, Column rhs, String op, String andOr) {
        this(lhs, rhs, op)
        setAndOr(andOr)
    }
    
    public void processJoinColumns(Schema root) {
        def leftTable = getCritJoin().table
        if (!leftTable) {
            leftTable = root.findTable(getCritJoin().name)
        }
        lhs = leftTable.findColumn(leftColumn).column
        try {
            def rh = getCritJoin().target.findColumn(rightColumn)
            if (rh) {
                rhs = rh.column
                if (rh.join) {
                    additionalJoin = rh.join
                }
            }
        } catch (ColumnNotFoundException ex) {
            log.info(ex.getMessage())
        }
        criteria.each {
            it.processJoinColumns(root)
        }
    }
     
    void setAndOr(String andOr) {
        this.@andOr = andOr.toUpperCase()
    }

    GString sql(boolean useValue = true) {
        boolean conjunction = getLhsId()
        GString sql = conjunction?"${getLhsId()} $op ${getRhsId(useValue)}":"${''}"
        criteria.each { 
            sql += "${conjunction?' ' + it.andOr + ' ':''}(${it.sql(useValue)})" 
            conjunction = true
        }
        return sql
    }
    
    List params() {
        List params = []
        if (rightColumn) {
            params << rightColumn
        }
        criteria.each { params.addAll(it.params()) }
        return params
    }
    
    public JoinBase getCritJoin() {
        if (critJoin) {
            return critJoin
        } else if (parent) {
            return parent.critJoin
        } else {
            return null
        }
    }
    
    public HashSet getAdditionalJoins() {
        def joins = new LinkedHashSet()
        if (additionalJoin) {
            joins << additionalJoin
        }
        criteria.each {
            joins.addAll(it.getAdditionalJoins())
        }
        return joins
    }
    
    public String getLhsId() {
        if (lhs) {
            return lhs.id()
        } else {
            return leftColumn
        }
    }
    
    public String getRhsId(boolean useValue = true) {
        if (useValue) {
            if (rhs) {
                return rhs.id()
            } else {
                return rightColumn
            }
        } else {
            return "?"
        }
    }
    
    public String toSqlMapDSL(int indentCount) {
        String dsl = ""
        String subDSL = "['$leftColumn', '$op', '$rightColumn']"
        if (!parent) {
            dsl += "${indent(3)}on($subDSL)${criteria?' {\n':'\n'}"
        } else {
            dsl += "($subDSL)${criteria?' {\n':'\n'}"
        }
        criteria.each {
            dsl += "${indent(indentCount + 2)}${it.andOr.toLowerCase()}${it.toSqlMapDSL(indentCount + 1)}" 
        }
        return dsl += "${indent(indentCount)}}\n"
    }
    
}


