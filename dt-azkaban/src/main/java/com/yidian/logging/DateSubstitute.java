package com.yidian.logging;

import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* 用于实现日期替换, 将满足${day, -1 day}, ${day}, ${day, -1 hour} 替换成真实的时间
*/
public class DateSubstitute {
    //private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
    private static Pattern varPat = Pattern.compile("\\$\\{[^\\}]*\\}");
    private final String baseDate;

    public DateSubstitute(String date) {
        this.baseDate = date;
    }

    public static DependencyChecker.VariableInfo getVariableInfo(String var) {
        var = var.substring(2, var.length() - 1); // remove ${ .. }
        String[] parts = var.split(",");

        DependencyChecker.VariableInfo variableInfo = new DependencyChecker.VariableInfo();
        variableInfo.name = parts[0];
        if (parts.length == 2) {
            variableInfo.parameter = parts[1].trim();
        }
        return variableInfo;
    }

    // ${date, -1 hour}
    public String getVariable(String var) {
        Preconditions.checkArgument(var != null);
        DependencyChecker.VariableInfo variableInfo = getVariableInfo(var);
        long offsetInSecond = 0;
        if (variableInfo.name.equals("day") || variableInfo.name.equals("date") || variableInfo.name.equals("hour")) {
            if (variableInfo.parameter != null) {
                String[] offsets = variableInfo.parameter.split("\\s");
                if (offsets[1].equalsIgnoreCase("day")) {
                    if (offsets[0].startsWith("+")) {  // +1
                        offsets[0] = offsets[0].substring(1);
                    }
                    offsetInSecond = Integer.parseInt(offsets[0]) * 24 * 3600;
                } else if (offsets[1].equalsIgnoreCase("hour")) {
                    offsetInSecond = Integer.parseInt(offsets[0]) * 3600;
                } else {
                    throw new UnsupportedOperationException("unknown type " + offsets[1]);
                }
            }
            long timestamp = Utilities.toTimestamp(baseDate) + offsetInSecond * 1000;
            // p_day >= 2014-08-22 and p_hour >= ${HOUR}
            if (variableInfo.name.equalsIgnoreCase("day")) {
                return Utilities.toDateTime(timestamp, "yyyy-MM-dd");
            } else if (variableInfo.name.equals("date")) {
                return Utilities.toDateTime(timestamp, "yyyy-MM-dd HH:mm:ss");
            } else { // hour
                return Utilities.toDateTime(timestamp, "HH");
            }
        }
        return var;
    }

    public String substitute(String expr) {
        Matcher match = varPat.matcher("");
        StringBuffer result = new StringBuffer();
        String eval = expr;
        while (true) {
            match.reset(eval);
            if (!match.find()) {
                result.append(eval);
                break;
            }
            String var = match.group();
            String val = getVariable(var);
            // substitute
            result.append(eval.substring(0, match.start()) + val);
            eval = eval.substring(match.end());
        }
        return result.toString();
    }
}
