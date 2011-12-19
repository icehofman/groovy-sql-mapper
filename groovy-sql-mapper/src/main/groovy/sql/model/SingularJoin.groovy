package sql.model

class SingularJoin extends Join {

    public SingularJoin() {
        super();
    }

    public SingularJoin(String alias, String name) {
        super(alias, name);
    }

    String toSqlMapDSL() {
        return super.toSqlMapDSL('singular')
    }
}
