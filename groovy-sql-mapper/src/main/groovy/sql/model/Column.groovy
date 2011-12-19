package sql.model

class Column extends ColumnBase {

    final static def literalRules = [
        new NotStringRule(),
        new NumberRule(),
        new SingleQuoteRule(),
        new SpecialCharacterRule()
    ]

    public Column() {
        super();
    }

    public Column(String alias, String name) {
        super(alias, name);
    }

    String projection(String dOpen = '"', String dClose = '"') {
        String id = id()
        String ref = reference(dOpen, dClose)
        return "$id as $ref"
    }

    String id() {
        String id
        if (table) {
            id = "${table.alias}.${name}"
        } else {
            id = "${name}"
        }

        if (aggFunction) {
            return "$aggFunction($id)"
        } else {
            return id
        }
    }

    Boolean isAggregate() {
        return agg || aggFunction
    }

    String reference(String dOpen = '"', String dClose = '"') {
        return "${dOpen + alias + dClose}"
    }

    synchronized boolean isLiteral() {
        if (literal == null) {
            literal = isLiteral(name)
        }
        return literal
    }

    public static boolean isLiteral(value) {
        for (def rule : literalRules) {
            try {
                if (rule.isLiteral(value)) {
                    return true
                }
            } catch (Exception e) {
            }
        }
        return false
    }
}


class NumberRule {

    boolean isLiteral(value) {
        if (value instanceof Number) {
            return true
        } else if (Integer.parseInt(value)) {
            return true
        } else if (Double.parseDouble(value)) {
            return true
        } else {
            return false
        }
    }
}

class NotStringRule {

    boolean isLiteral(value) {
        return !(value instanceof String)
    }
}

class SingleQuoteRule {

    boolean isLiteral(value) {
        return value.startsWith("'") && value.endsWith("'")
    }
}

class SpecialCharacterRule {
    boolean isLiteral(value) {
        return !(value ==~ /^[A-Za-z._0-9]*$/)
    }
}

class StringWithDotRule {
    boolean isLiteral(value) {
        return value.contains(".")
    }
}
