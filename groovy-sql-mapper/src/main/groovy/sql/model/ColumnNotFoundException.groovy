package sql.model

class ColumnNotFoundException extends RuntimeException {

    public ColumnNotFoundException() {
        super();
    }

    public ColumnNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ColumnNotFoundException(String message) {
        super(message);
    }

    public ColumnNotFoundException(Throwable cause) {
        super(cause);
    }
    
}
