package com.yidian.logging;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by sunshangchun on 14-9-5.
 */
public class TestAzkabanPlugin {
    @Test
    public void testGetDate() throws IOException {
        System.err.println(AzkabanPlugin.calculateDateTime());
    }
}
