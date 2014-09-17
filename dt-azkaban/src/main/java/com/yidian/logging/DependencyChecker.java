package com.yidian.logging;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by sunshangchun on 14-9-3.
 */
public class DependencyChecker {
    private static final Logger logger = LoggerFactory.getLogger(DependencyChecker.class);
    private static final String FREQUENCY_KEY = "freq";
    private static final String DAY_PARTKEY = "p_day";
    private static final String HOUR_PARTKEY = "p_hour";

    public static class Dependency {
        String dbName;
        String tableName;
        String beginDate;
        String endDate;
    }

    public static class VariableInfo {
        public String name;
        public String parameter;
    }

    private int getFrequency(Table table) {
        if (table.getParameters().containsKey(FREQUENCY_KEY)) {
            return Integer.parseInt(table.getParameters().get(FREQUENCY_KEY).trim());
        }
        List<FieldSchema> partKeys = table.getPartitionKeys();
        for (int i = 0; i < partKeys.size(); ++i) {
            if (partKeys.get(i).getName().equals(HOUR_PARTKEY)) {
               return 60;
            }
        }
        return 1440;
    }

    public static String getFilter(String startDate, String endDate, boolean existsHourKey) {
        String startDay = Utilities.formatDateTime(startDate, "yyyy-MM-dd");
        String lastDay = Utilities.formatDateTime(endDate, "yyyy-MM-dd");
        if (startDay.equals(lastDay)) {
            String startHour = Utilities.formatDateTime(startDate, "HH");
            String endHour = Utilities.formatDateTime(endDate, "HH");
            String filter = String.format("%s = '%s'", DAY_PARTKEY, startDay);
            if (existsHourKey) {
                filter += String.format(" and %s >= '%s' and %s < '%s'", HOUR_PARTKEY, startHour, HOUR_PARTKEY, endHour);
            }
            return filter;
        } else {
            return String.format("%s >= '%s' and %s < '%s'", DAY_PARTKEY, startDay, DAY_PARTKEY, lastDay);
        }
    }

    // a.dep
    // dw.dim-user-info={'from': ${date, -1 hour}, 'to': '${date}'}
    public boolean isReady(Dependency dependency) {
        try {
            HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf());
            Table table = client.getTable(dependency.dbName, dependency.tableName);
            List<FieldSchema> partKeys = table.getPartitionKeys();
            int p_day_index = -1;
            int p_hour_index = -1;
            for (int i = 0; i < partKeys.size(); ++i) {
                if (partKeys.get(i).getName().equals(DAY_PARTKEY)) {
                    p_day_index = i;
                } else if (partKeys.get(i).getName().equals(HOUR_PARTKEY)) {
                    p_hour_index = i;
                }
            }
            int freq = getFrequency(table);
            String dateFormat;
            if (freq == 1440) {
                dateFormat = "yyyy-MM-dd";
            } else {
                dateFormat = "yyyy-MM-dd HH";
            }
            String filter = getFilter(dependency.beginDate, dependency.endDate, p_hour_index >= 0);
            logger.info("filter=" + filter);
            long beginTime = Utilities.toTimestamp(dependency.beginDate);
            long endTime = Utilities.toTimestamp(dependency.endDate);
            List<Partition> partitions = client.listPartitionsByFilter(dependency.dbName, dependency.tableName, filter, (short) -1);
            logger.info("Get Partitions Count " + partitions.size());
            int numCount = 0;
            Set<String> partNames = new HashSet<String>();
            for (Partition partition : partitions) {
                List<String> values = partition.getValues();
                String date = values.get(p_day_index);
                if (p_hour_index > 0) {
                    date += " " + values.get(p_hour_index) + ":00";
                }
                long partTime = Utilities.toTimestamp(date);
                if (beginTime <= partTime && partTime < endTime) {
                    partNames.add(Utilities.toDateTime(partTime, dateFormat));
                    ++numCount;
                }
            }
            long expectedCount = (endTime - beginTime) / (60 * 1000) / freq;
            if (numCount < expectedCount) {
                logger.info("only found " + numCount + " expected count " + expectedCount);
                return false;
            }
            for (long timeStamp = beginTime; timeStamp < endTime; timeStamp += freq * 60 * 1000) {
                if (!partNames.contains(Utilities.toDateTime(timeStamp, dateFormat))) {
                    logger.info("not found partition " + Utilities.toDateTime(timeStamp));
                    return false;
                }
            }
            return true;
        } catch (TTransportException e) {
            logger.warn(e.getMessage());
        } catch (MetaException e) {
            logger.warn("failed to check ready", e);
        } catch (TException e) {
            logger.warn(e.getMessage());
        }
        return false;
    }

    public List<Dependency> loadDependencies(String baseDate, String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        DateSubstitute dateSubstitute = new DateSubstitute(baseDate);
        List<Dependency> dependencies = new ArrayList<Dependency>();
        while ((line = reader.readLine()) != null) {
            Dependency dependency = new Dependency();
            String[] parts = line.split("=");
            {
                String[] names = parts[0].split("\\.");
                dependency.dbName = names[0];
                dependency.tableName = names[1];
            }
            {
                JsonObject filter = new JsonParser().parse(parts[1]).getAsJsonObject();
                String startDate = filter.get("from").getAsString();
                String endDate = filter.get("to").getAsString();
                dependency.beginDate = dateSubstitute.getVariable(startDate);
                dependency.endDate = dateSubstitute.getVariable(endDate);
            }
            dependencies.add(dependency);
        }
        return dependencies;
    }

    public void run(String baseDate, String depConfFileName) throws IOException {
        logger.info("begin to check dependencies");
        List<Dependency> dependencies = loadDependencies(baseDate, depConfFileName);
        final int SLEEP_INTERVAL = 60 * 1000;
        while (true) {
            boolean isAllReady = true;
            List<Dependency> swapDependencies = new ArrayList<Dependency>(dependencies);
            for (Dependency dependency : swapDependencies) {
                if (!isReady(dependency)) {
                    logger.info(dependency.dbName + "." + dependency.tableName + "/{" + dependency.beginDate
                            + "," + dependency.endDate + "} is not ready");
                    isAllReady = false;
                    break;
                } else {
                    logger.info(dependency.dbName + "." + dependency.tableName + "/{" + dependency.beginDate
                            + "," + dependency.endDate + "} is ready");
                    dependencies.remove(dependency);
                }
            }
            if (isAllReady) {
               break;
            }
            try {
                Thread.sleep(SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String depFileName = args[0];
        String baseDate = AzkabanPlugin.calculateDateTime();

        DependencyChecker jobDependency = new DependencyChecker();
        jobDependency.run(baseDate, depFileName);
        logger.info("Is All Ready Now");
    }
}
