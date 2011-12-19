package sql;
import org.junit.Test 
import sql.model.Column 

import static org.junit.Assert.*;

class CriteriaTest {
    
    @Test
    void literalsRules() {
        assert false == Column.isLiteral("column")
        assert true == Column.isLiteral("5")
        assert true == Column.isLiteral("'column'")
        assert false == Column.isLiteral("myTable.myColumn")
    }
}
