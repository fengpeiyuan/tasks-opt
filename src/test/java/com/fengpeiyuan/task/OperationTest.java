package com.fengpeiyuan.task;


import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationTest {
    private ClassPathXmlApplicationContext context;

    @Before
    public void setup(){
        context = new ClassPathXmlApplicationContext(new String[] {"spring-config-redis.xml"});
        context.start();
    }



    @Test
    public void testFillMaintaskAndSubtasks(){
        Operation operation = (Operation)context.getBean("operation");
        Boolean ret = operation.fillMaintaskAndSubtasks("q",9,99);
        assertTrue(ret==Boolean.TRUE);


    }


}
