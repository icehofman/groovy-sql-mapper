package sql.model

class QueryDefinitionException extends RuntimeException {

    public QueryDefinitionException() {
        super();
    }

    public QueryDefinitionException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryDefinitionException(String message) {
        super(message);
    }

    public QueryDefinitionException(Throwable cause) {
        super(cause);
    }
    
}
