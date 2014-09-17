package com.yidian.logging;


import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class PrepareQuery {
    public static Logger logger = LoggerFactory.getLogger(PrepareQuery.class);

    public static void main(String[] args) throws IOException {
        logger.info("begin to replace query ");
        String baseDate = AzkabanPlugin.calculateDateTime();
        DateSubstitute dateSubstitute = new DateSubstitute(baseDate);
        String input = args[0];
        String output;
        if (args.length == 1) {
            output = args[0] + ".out";
        } else {
            output =  args[1];
        }
        StringBuffer sb = new StringBuffer();
        FileWriter writer = new FileWriter(output);
        try {
            List<String> lines = Files.readLines(new File(input), Charset.forName("UTF-8"));
            for (String line : lines) {
                sb.append(dateSubstitute.substitute(line));
                sb.append("\n");
            }
            writer.append(sb.toString());
        } finally {
            writer.close();
        }
        logger.info("end to prepare");
    }
}
