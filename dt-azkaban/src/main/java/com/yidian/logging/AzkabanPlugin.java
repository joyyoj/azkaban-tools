package com.yidian.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by sunshangchun on 14-9-5.
 */
public class AzkabanPlugin {
    private static final String AzkabanFlowStartTimestamp = "azkaban.flow.start.timestamp";
    private static final String AzkabanPropFileNameEnv = "JOB_PROP_FILE";
    private static final String AzkabanJobOuputPropFileNameEnv = "JOB_OUTPUT_PROP_FILE";
    private static final String AzkabanDate = "date";
    private static Logger logger = LoggerFactory.getLogger(AzkabanPlugin.class);

    public static String calculateDateTime() throws IOException {
        Properties properties = new Properties();
        String inputFile = System.getenv(AzkabanPropFileNameEnv);
        if (inputFile != null) {
            properties.load(new FileInputStream(new File(inputFile)));
        }
        String baseDate = properties.getProperty(AzkabanDate);
        if (baseDate != null) {
            baseDate = Utilities.formatDateTime(baseDate);
        } else {
            String execDayStr = properties.getProperty(AzkabanFlowStartTimestamp);
            logger.info("current exec day str " + execDayStr);
            long flowStartTime = System.currentTimeMillis();
            if (execDayStr != null) {
//                execDayStr = "2014-09-04T12:19:38.189+08:00";
                execDayStr = execDayStr.substring(0, execDayStr.lastIndexOf("."));
                execDayStr = execDayStr.replace('T', ' ');
                flowStartTime = Utilities.toTimestamp(execDayStr);
            }
            baseDate = Utilities.toDateTime(flowStartTime, "yyyy-MM-dd HH:00:00");
        }
        logger.info("current base date " + baseDate);
        return baseDate;
    }
}
