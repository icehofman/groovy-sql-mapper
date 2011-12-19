package sql.model;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SqlModelBase implements Cloneable, Serializable {
    
    String name;
    String alias;
    
    public SqlModelBase(String alias, String name) {
        super();
        this.alias = alias;
        this.name = name;
    }

    public SqlModelBase() {
        super();
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAliasDot() {
        if (alias != null && alias.length() > 0) {
            return alias + ".";
        } else {
            return null;
        }
    }
    
    public String indent(int x) {
        String indent = "";
        for (int i = 0; i < x; i++) {
            indent += "    ";
        }
        return indent;
    }
}
