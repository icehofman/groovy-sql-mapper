package sql.model

class Join extends JoinBase {

    public Join() {
        super();
    }

    public Join(String alias, String name) {
        super(alias, name);
    }

    String declaration() {
        String declaration
        if (left) {
            declaration = "LEFT OUTER JOIN "
        } else if (inner) {
            declaration = "INNER JOIN "
        } else {
            declaration = "CROSS JOIN "
        }
        declaration += "${table.reference()}"
        if (criteria) {
            declaration += " ON\n    (\n        ${criteria.sql()}\n    )"
        }
        return declaration
    }

    String toSqlMapDSL(String type) {
        String dsl = "${indent(2)}$type(name: '$name', alias: '$alias', $joinTypeDSL) {\n"
        dsl += criteria.toSqlMapDSL(2);
    }

    String getJoinTypeDSL() {
        if (left) {
            return "left: true"
        } else if (inner) {
            return "inner: true"
        } else {
            return "cross: true"
        }
    }
}
