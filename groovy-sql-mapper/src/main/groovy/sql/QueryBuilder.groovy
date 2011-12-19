package sql

import groovy.sql.Sql 
import java.util.Map
import org.apache.commons.logging.Log 
import org.apache.commons.logging.LogFactory 
import sql.model.Column 
import sql.model.ColumnNotFoundException 
import sql.model.Criteria 
import sql.model.QueryDefinitionException 
import sql.model.Schema
import sql.model.SingularJoin;
import sql.model.Table 

public class QueryBuilder extends BuilderSupport {
    
    private final static Log log = LogFactory.getLog(QueryBuilder.class)
    
    def dOpen = '"'
    def dClose = '"'
    Schema schema
    Table root
    boolean hasAgg
    def columns = []
    def orderBy = [:]
    Criteria criteria
    Sql sql
    HashSet joins = new LinkedHashSet()
    
    public QueryBuilder(Schema schema) {
        this.schema = schema
    }
    
    public QueryBuilder(Schema schema, Sql sql) {
        this.schema = schema
        this.sql = sql
    }
    
    
    /**
     * Returns a List of clazz objects, for each row returned by the current query
     */
    public <T> List<T> execute(Sql sql, Class<T> clazz, int offset = 0, int maxRows = 0) {
        return execute(sql, offset, maxRows) { row ->
            return clazz.newInstance(row.toRowResult())
        }
    }
    
    /**
     * Returns a List of clazz objects, for each row returned by the current query
     */
    public <T> List<T> execute(Class<T> clazz, int offset = 0, int maxRows = 0) {
        return execute(sql, offset, maxRows) { row ->
            return clazz.newInstance(row.toRowResult())
        }
    }
    
    /**
     * Returns a List<Map> with each row returned by the current query.
     */
    public List execute(Sql sql, int offset = 0, int maxRows = 0) {
        return execute(sql, offset, maxRows) { row ->
            return row.toRowResult()
        }
    }
    
    public List execute(int offset = 0, int maxRows = 0) {
        return execute(sql, offset, maxRows)
    }
    
    /**
     * Returns a List containing each object returned from the rowHandler.  The rowHandler is applied to each row 
     * returned from the current query.
     */
    public List execute(Sql sql, int offset = 0, int maxRows = 0, Closure rowHandler) {
        def result = []
        def select = select()
        def params
        if (criteria) {
            params = criteria.params()
        }
        if (log.isDebugEnabled()) {
            log.debug("executing: $select  \n params: $params")
        }
        if (params) {
            sql.eachRow(select, params, offset, maxRows) { result << rowHandler(it) }
        } else {
            sql.eachRow(select, offset, maxRows) { result << rowHandler(it) }
        }
        return result
    }
    
    /**
     * Returns a List containing each object returned from the rowHandler.  The rowHandler is applied to each row
     * returned from the current query.
     */
    public List execute(int offset = 0, int maxRows = 0, Closure rowHandler) {
        return execute(sql, offset, maxRows, rowHandler)
    }
    
    String select() {
        String select = "\nSELECT\n"
        def columns = this.columns
        columns.eachWithIndex { it, idx ->
            select += "    ${it.projection(dOpen, dClose)}${(columns.size() > idx + 1)?',':''}\n"
        }
        select += "FROM ${root.reference()}\n"
        def joins = findJoins()
        if (joins) {
            joins.each { select += "    ${it.declaration()}\n" }
        }
        if (criteria) {
            select += "WHERE ${criteria.sql(false)}\n"
        }
        if (hasAggregate() && nonAggregate()) {
            select += "GROUP BY "
            select += columns.collect({
                if (!it.aggregate) {
                    return it.id()
                }
            }).findAll({ it }).join(", ")
            select += "\n"
        }
        if (orderBy) {
            select += "ORDER BY "
            select += orderBy.collect({ name, order ->
                return "$name $order"
            }).join(", ")
            select += "\n"
        }
        return select
    }
    
    def nonAggregate() {
        for (Column column : columns) {
            if (!column.aggregate) {
                return true
            }
        }
    }
    
    def hasAggregate() {
        for (Column column : columns) {
            if (column.aggregate) {
                return true
            }
        }
    }

    
    HashSet findJoins() {
        columns.each {
            if (it.table && it.table != root) {
                def join = root.findJoin(it.table.alias)
                joins << join
                if (join.criteria) {
                    def additionalJoins = join.criteria.additionalJoins
                    if (additionalJoins) {
                        joins.addAll(additionalJoins.findAll { it != null })
                    }
                }
            }
        }
        return joins
    }
    
    @Override
    protected void setParent(Object parent, Object child) {
        if (parent instanceof Criteria) {
            parent.criteria += child
            child.parent = parent
        }
    }
    
    @Override
    protected Object createNode(Object name) {
        if (!root) {
            findRoot(name)
            columns = root.columns
            return root
        } else {
            return addCriteria(name, null)
        }
    }
    
    private findRoot(name) {
        root = schema.findTable(name)
        if (!root) {
            throw new QueryDefinitionException("Table '$name' not found in schema '$schema.name'")
        }
    }
    
    @Override
    protected Object createNode(Object name, Object value) {
        if (!root) {
            findRoot(name)
            findOrderBy(value)
            if (value) {
                columns = root.findColumns(value)
            } else {
                columns = root.columns
            }
            return root
        } else {
            if (["where", "and", "or"].contains(name.toLowerCase())) {
                return addCriteria(name, value)
            } else if (name.equalsIgnoreCase("join")) {
                def join = root.findJoin(value)
                if (join) {
                    joins << join
                } else {
                    throw new QueryDefinitionException("$root.alias does not have a join referenced with $value")
                }
                return join
            } else {
                return addAggregate(name, value)
            }
        }
    }
    
    private addCriteria(String name, value) {
        if (value instanceof String) {
            def desc = root.findColumn(value)
            Column column = desc.column
            joins << desc.join
            value = column.reference(dOpen, dClose)
        } else if (value instanceof List && value.size() != 0 && value.size() <= 3) {
            def idxs = (value.size() == 3)?[0, 2]:0..(value.size()-1)
            for (int idx : idxs) {
                if (!Column.isLiteral(value[idx])) {
                    try {
                        def desc = root.findColumn(value[idx])
                        value[idx] = desc.column.id()
                        if (desc.join) {
                            joins << desc.join
                        }
                    } catch (ColumnNotFoundException e) {
                        if (idx == 0) {
                            // left-hand side must be a valid column or db literal
                            throw e
                        }
                    }
                }
            }
        } else if (value == null) {
            // do nothing with this
        } else {
            throw new QueryDefinitionException("invalid criteria definition: $value")
        }
        Criteria crit = new Criteria(value)
        if (name.equals("where")) {
            this.criteria = crit
        } else {
            crit.setAndOr(name)
        }
        return crit
    }
    
    private addAggregate(name, value, alias = null) {
        def agg = root.findColumn(value)
        Column column 
        if (agg) {
            if (agg.join instanceof SingularJoin) {
                throw new RuntimeException("Cannot aggregate over singular relationship: ${join.target.alias} -> ${join.table.alias} ($value)")
            }
            column = agg.column
        } else {
            log.info("Could not find aggregate column '$value', it will be used literally.")
            if (!alias) {
                throw new RuntimeException("Cannot have a literal in an aggregate without an alias.")
            }
            column = new Column("name": (value ==~ /.*[\+\-]$/)?value[0..-2]:value, "agg" : true)
        }
        column.aggFunction = name
        column.alias = alias?alias:column.alias
        columns += column
        findOrderBy(value, column)
        return column
    }
    
    def findOrderBy(String value, Column column = null) {
        // alias will come from the column provided, or the value minus the +,-
        if (!column) {
            column = root.findColumn(value)?.column
        }
        String id = column?column.id():value[0..-2]
        if (value.endsWith("+")) {
            orderBy.put(id, "asc")
        } else if (value.endsWith("-")) {
            orderBy.put(id, "desc")
        }
    }
    
    def findOrderBy(List values) {
        values.each {
            def column = root.findColumn(it)?.column
            findOrderBy(it, column)
        }
    }
    
    @Override
    protected Object createNode(Object name, Map attributes) {
        log.debug("creating node")
    }
    
    @Override
    protected Object createNode(Object name, Map attributes, Object value) {
        addAggregate(name, value, attributes.alias)
    }
}