package sql

import groovy.util.FactoryBuilderSupport

import java.util.Map

import org.apache.commons.logging.Log 
import org.apache.commons.logging.LogFactory 
import sql.model.Column
import sql.model.Criteria 
import sql.model.PluralJoin 
import sql.model.Schema 
import sql.model.SingularJoin 
import sql.model.SqlMapDefinitionException;
import sql.model.Table 

class SqlMapBuilder extends FactoryBuilderSupport {
    
    Log log = LogFactory.getLog(SqlMapBuilder.class)
    Schema root
    
    public SqlMapBuilder(boolean init = true) {
        super(init)
    }
        
    def registerObjectFactories() {
        registerFactory("schema", new SchemaFactory())
        registerFactory("table", new TableFactory())
        registerFactory("singular", new SingularFactory())
        registerFactory("plural", new PluralFactory())
        registerFactory("column", new ColumnFactory())
        registerFactory("on", new OnFactory())
        registerFactory("and", new AndFactory())
        registerFactory("or", new OrFactory())
    }
}

class SchemaFactory extends AbstractFactory {
    
    Log log = LogFactory.getLog(SqlMapBuilder.class)
    
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        builder.root = new Schema("name" : value)
        return builder.root
    }

    @Override
    public void onNodeCompleted(FactoryBuilderSupport builder, Object parent, Object node) {
        node.tables.each { table ->
            (table.singulars + table.plurals).each { joinBase ->
                Table joinTable = node.findTable(joinBase.name)
                if (joinTable) {
                    joinBase.table = joinTable
                    if (!joinBase.alias) {
                        joinBase.alias = joinTable.alias
                    }
                    if (joinBase.criteria) {
                        joinBase.criteria.processJoinColumns(node)
                    }
                } else {
                    throw new SqlMapDefinitionException("Could not find join table '${joinBase.name}'")
                }
            }
        }
        
    }            
            
}


class TableFactory extends AbstractFactory {
    
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        return new Table(attributes)
    }
    
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && parent instanceof Schema) {
            child.schema = parent
            parent.tables += child
        } else {
            throw new RuntimeException("Table must have a Schema parent")
        }
    }
}

class ColumnFactory extends AbstractFactory {
    
    @Override
    public boolean isLeaf() {
        return true
    }
    
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        return new Column(attributes)
    }
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && parent instanceof Table) {
            child.table = parent
            parent.columns += child
        } else {
            throw new RuntimeException("Column must have a Table parent")
        }
    }
}

class SingularFactory extends AbstractFactory {
    
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        return new SingularJoin(attributes)
    }
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && parent instanceof Table) {
            child.target = parent
            parent.singulars += child
        } else {
            throw new RuntimeException("Singular must have a Table parent")
        }
    }
}

class PluralFactory extends AbstractFactory {
    
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        return new PluralJoin(attributes)
    }
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && parent instanceof Table) {
            child.target = parent
            parent.plurals += child
        } else {
            throw new RuntimeException("Plural must have a Table parent")
        }
    }
}

class OnFactory extends AbstractFactory {
        
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        if (value) {
            return new Criteria(value)
        } else {
            return new Criteria(attributes)
        }
    }
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && (parent instanceof SingularJoin || parent instanceof PluralJoin)) {
            parent.criteria = child
            child.critJoin = parent
        } else {
            throw new RuntimeException("On must have a Join (singular or plural) parent")
        }
    }
}

class OrFactory extends AbstractFactory {
    
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        Criteria crit
        if (value) {
            crit = new Criteria(value)
        } else {
            crit = new Criteria(attributes)
        }
        crit.andOr = "or"
        return crit
    }
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && (parent instanceof Criteria)) {
            parent.criteria += child
            child.parent = parent
        } else {
            throw new RuntimeException("Or must have a Criteria (on, or, and) parent")
        }
    }
}

class AndFactory extends AbstractFactory {
    public Object newInstance(FactoryBuilderSupport builder, Object name, Object value, Map attributes)
            throws InstantiationException, IllegalAccessException {
        Criteria crit
        if (value) {
            crit = new Criteria(value)
        } else {
            crit = new Criteria(attributes)
        }
        crit.andOr = "and"
        return crit
    }
    
    @Override
    public void setParent(FactoryBuilderSupport builder, Object parent, Object child) {
        if (parent != null && (parent instanceof Criteria)) {
            parent.criteria += child
            child.parent = parent
        } else {
            throw new RuntimeException("And must have a Criteria (on, or, and) parent")
        }
    }
}




