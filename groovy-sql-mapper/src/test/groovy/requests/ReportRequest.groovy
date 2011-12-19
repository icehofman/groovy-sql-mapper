package requests

class ReportRequest {
    
    String tableName
    Map filters = [:]
    List columns = []
    Map sums = [:]
    Map counts = [:]
        
}
