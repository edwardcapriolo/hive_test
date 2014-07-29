package io.teknek.hiveunit.common;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ResponseTest {

    @Test
    public void testGetStatusCodeSetAtConstructor() throws Exception {
        List<String> someList = Lists.newArrayList();
        ResponseStatus status = ResponseStatus.SUCCESS;
        Response response = new Response(status, someList);

        assertEquals(status, response.getResponseStatus());
        assertEquals(someList, response.getResult());
    }

    @Test
    public void testGetStatusCodeFromZeroOnConstructor() throws Exception {
        Response response = new Response(0, Lists.<String>newArrayList());

        assertEquals(ResponseStatus.SUCCESS, response.getResponseStatus());
    }

    @Test
    public void testGetStatusCodeFromNonZeroOnConstructor() throws Exception {
        Response response = new Response(-1, Lists.<String>newArrayList());

        assertEquals(ResponseStatus.FAILURE, response.getResponseStatus());
    }

    @Test
    public void testGetStatusCodeFromNullOnConstructor() throws Exception {
        Response response = new Response(null, Lists.<String>newArrayList());

        assertEquals(ResponseStatus.FAILURE, response.getResponseStatus());
    }

    @Test()
    public void testGetResult() throws Exception {
        List<String> someList = Lists.newArrayList();
        Response response = new Response(ResponseStatus.SUCCESS, someList);

        assertEquals(someList, response.getResult());
    }

    @Test()
    public void testGetResultEmptyListOnNullConstructorParam() throws Exception {
        Response response = new Response(ResponseStatus.SUCCESS, null);

        assertEquals(Lists.<String>newArrayList(), response.getResult());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetResultImmutable() throws Exception {
        List<String> someList = Lists.newArrayList();
        Response response = new Response(ResponseStatus.SUCCESS, someList);

        assertEquals(someList, response.getResult());
        response.getResult().add("someString");
    }
}
