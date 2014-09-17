package com.yidian.logging;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by sunshangchun on 14-9-5.
 */
public class TestDateSubstitue {
    @Test
    public void testGetVariable() {
        DateSubstitute dateSubstitute = new DateSubstitute("2014-09-01 12:00");
        Assert.assertEquals("2014-09-01 12:00:00", dateSubstitute.getVariable("${date}"));
        Assert.assertEquals("2014-09-01", dateSubstitute.getVariable("${day}"));
        Assert.assertEquals("12", dateSubstitute.getVariable("${hour}"));

        Assert.assertEquals("08", dateSubstitute.getVariable("${hour, -4 hour}"));
        Assert.assertEquals("2014-08-31", dateSubstitute.getVariable("${day, -1 day}"));
        Assert.assertEquals("2014-09-01 00:00:00", dateSubstitute.getVariable("${date, -12 hour}"));
        Assert.assertEquals("2014-09-02 12:00:00", dateSubstitute.getVariable("${date, 1 day}"));
        Assert.assertEquals("2014-09-02 12:00:00", dateSubstitute.getVariable("${date, +1 day}"));
    }

    @Test
    public void testSubstitute() {
        DateSubstitute dateSubstitute = new DateSubstitute("2014-09-01 11:00");
        String query = "p_day = '${day}' and p_day = '${date, -1 hour}', ${hour, -1 hour }";
        String ret = dateSubstitute.substitute(query);
        Assert.assertEquals("p_day = '2014-09-01' and p_day = '2014-09-01 10:00:00', 10", ret);

        query = "$DAY, not ${DAY} ${VAR} ${${day, -1}}";
        ret = dateSubstitute.substitute(query);
        Assert.assertEquals(query, ret);
    }
}
