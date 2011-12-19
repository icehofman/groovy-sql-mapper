package sql.model

class SqlMapDefinitionException extends RuntimeException {

    public SqlMapDefinitionException() {
        super();
    }

    public SqlMapDefinitionException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlMapDefinitionException(String message) {
        super(message);
    }

    public SqlMapDefinitionException(Throwable cause) {
        super(cause);
    }

}
