package com.yidian.logging;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by sunshangchun on 14-9-5.
 */
public class TestDependencyChecker {
    @Test
    public void testGetFilterNoHour() {
        String filter = DependencyChecker.getFilter("2014-09-01 00:00", "2014-09-01 05:00", false);
        Assert.assertEquals("p_day = '2014-09-01'", filter);
    }

    @Test
    public void testGetFilterSingleDayHasHour() {
        String filter = DependencyChecker.getFilter("2014-09-01 00:00", "2014-09-01 05:00", true);
        Assert.assertEquals("p_day = '2014-09-01' and p_hour >= '00' and p_hour < '05'", filter);
    }

    @Test
    public void testGetFilterMultiDayHasHour() {
        String filter = DependencyChecker.getFilter("2014-08-01 07:00", "2014-09-01 05:00", true);
        Assert.assertEquals("p_day >= '2014-08-01' and p_day < '2014-09-01'", filter);
    }

    @Test
    public void testGetFilterMultiDayHasHour2() {
        String filter = DependencyChecker.getFilter("2014-08-31 07:00", "2014-09-01 05:00", true);
        Assert.assertEquals("p_day >= '2014-08-31' and p_day < '2014-09-01'", filter);
    }
}
