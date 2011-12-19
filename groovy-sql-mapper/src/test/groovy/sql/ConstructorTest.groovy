package sql;
import org.junit.Test 

import static org.junit.Assert.*;

class ConstructorTest {

    
    @Test
    void constructTest() throws Exception {
        PersonConstruct person = construct(PersonConstruct.class, [name: "dave", age: 5])
        assert person.name == "dave"
        assert person.age == 5
    }
    
    public <T> T construct(Class<T> clazz, Map args) {
        return clazz.newInstance(args)
    }
    
    
}

class PersonConstruct {
    def name
    def age
}
