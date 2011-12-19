package sql.model

class PluralJoin extends Join {

    public PluralJoin() {
        super();
    }

    public PluralJoin(String alias, String name) {
        super(alias, name);
    }

    String toSqlMapDSL() {
        return super.toSqlMapDSL('plural')
    }
}
