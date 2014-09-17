package com.yidian.logging;


import azkaban.executor.ExecutableFlow;
import azkaban.sla.SlaOption;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by sunshangchun on 14-9-12.
 */
public class SMSAlerter extends Emailer {
    private static final Logger logger = LoggerFactory.getLogger(SMSAlerter.class);
    private static final Pattern pat = Pattern.compile("([0-9]+)");
    private static final Pattern removeTagsPat = Pattern.compile("<.+?>");
    // 124.207.72.68
    private static final int DEFAULT_TIMEOUT = 10000;
    private static final int DEFAULT_RETRIES = 2;
    private static final String DEFAULT_PHONE_NAME = "sms.phone_number";
    private static String GSM_HOST_IP = "gsm.host";
    private static String GSM_HOST_PORT = "gsm.port";
    private static String SMS_TIMEOUT = "sms.timeout";
    private static String SMS_RETRIES = "sms.retries";
    private static String TEST_MODE = "test.mode";
    private boolean testMode;
    private List<String> defaultPhoneNumbers;
    private final String host;
    private final int port;
    private final int timeout;
    private final int retries;

    private String getURL(String phoneNumber, String msg) throws IOException {
       return "/gnokii/send?phone=" + phoneNumber + "&msg=" + URLEncoder.encode(msg, "UTF-8");
    }

    private void sendMessage(List<String> phoneNumbers, String msg) throws IOException {
        Set<String> phoneSet = new HashSet<String>(phoneNumbers);
        String phone = Joiner.on(",").join(phoneSet);
        for (int i = 0; i < retries; ++i) {
            try {
                logger.info("begin to send message " + msg + " to " + phone + " times:" + (i + 1));
                HttpParams httpParams = new BasicHttpParams();
                HttpConnectionParams.setConnectionTimeout(httpParams, timeout);
                HttpConnectionParams.setSoTimeout(httpParams, timeout);
                HttpHost proxy = new HttpHost(host, port);
                HttpClientParams.setRedirecting(httpParams, true); // 重定向，很重要的设置，不加不起作用

                HttpClient client = new DefaultHttpClient(httpParams);
                boolean success = true;
                Set<String> phoneSetCopy = new HashSet<String>(phoneSet);
                for (String phoneNumber : phoneSetCopy) {
                    if (testMode) {
                        logger.info("testMode: success to send message " + msg + " to " + phoneNumber);
                        phoneSet.remove(phoneNumber);
                    } else {
                        HttpGet request = new HttpGet(getURL(phoneNumber, msg));
                        HttpResponse response = client.execute(proxy, request);
                        logger.info("send message " + msg + " to " + phoneNumber + " response " + response.toString());
                        if (response.getStatusLine().getStatusCode() == 200) {
                            phoneSet.remove(phoneNumber);
                        } else {
                            success = false;
                        }
                    }
                }
                if (success) {
                    break;
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("send message interrupted ", e);
                Thread.interrupted();
            } catch (ClientProtocolException e) {
                logger.error("failed to send message " + msg + " to " + phone, e);
            } catch (IOException e) {
                logger.error("failed to send message " + msg + " to " + phone, e);
            }
        }
    }

//    private static Props prepareProps(Props props) {
//        // meaningless, just to skip not initialized error
//        props.put("server.port", 100);
//        props.put("server.useSSL", "false");
//        props.put("jetty.use.ssl", "false");
//        props.put("jetty.port", "0");
//        return props;
//    }

    public SMSAlerter(Props props) {
        super(props);
        defaultPhoneNumbers = Arrays.asList(props.getString(DEFAULT_PHONE_NAME, "").split(","));
        host = props.get(GSM_HOST_IP);
        port = props.getInt(GSM_HOST_PORT);
        timeout = props.getInt(SMS_TIMEOUT, DEFAULT_TIMEOUT);
        retries = props.getInt(SMS_RETRIES, DEFAULT_RETRIES);
        testMode = props.getBoolean("test.mode", false);
        Preconditions.checkArgument(defaultPhoneNumbers != null);
    }

    private String getDescription(ExecutableFlow exFlow) {
        String date = exFlow.getExecutionOptions().getFlowParameters().get("date");
        if (date == null) {
            return "[" + exFlow.getProjectId() + "." + exFlow.getId() + "]";
        } else {
            return "[" + exFlow.getProjectId() + "." + exFlow.getId() + " " + date + "]";
        }
    }

    private static boolean isPhoneNumber(String str) {
        if (pat.matcher(str).matches()) {
           return true;
        }
        return false;
    }

    private static void classify(List<String> strList, List<String> emails, List<String> phoneNumber) {
        for (String email : strList) {
            if (isPhoneNumber(email)) {
                phoneNumber.add(email.trim());
            } else {
                emails.add(email.trim());
            }
        }
    }


    @Override
    public void alertOnSuccess(ExecutableFlow exflow) throws Exception {
        String message = getDescription(exflow) + " is success";
        List<String> strs = exflow.getExecutionOptions().getSuccessEmails();
        List<String> emails = new ArrayList<String>();
        List<String> phoneNumbers = new ArrayList<String>();
        classify(strs, emails, phoneNumbers);
        sendMessage(phoneNumbers, message);
        try {
            exflow.getExecutionOptions().setSuccessEmails(emails);
            super.alertOnSuccess(exflow);
        } finally {
            exflow.getExecutionOptions().setSuccessEmails(strs);
        }
    }

    @Override
    public void alertOnError(ExecutableFlow exflow, String... extraReasons) throws Exception {
        String message = getDescription(exflow) + " is error";
        List<String> strs = exflow.getExecutionOptions().getFailureEmails();
        List<String> emails = new ArrayList<String>();
        List<String> phoneNumbers = new ArrayList<String>();
        classify(strs, emails, phoneNumbers);
        sendMessage(phoneNumbers, message);
        try {
            exflow.getExecutionOptions().setFailureEmails(emails);
            super.alertOnError(exflow);
        } finally {
            exflow.getExecutionOptions().setFailureEmails(strs);
        }
    }

    @Override
    public void alertOnFirstError(ExecutableFlow exflow) throws Exception {
        String message = getDescription(exflow) + " first error";
        List<String> strs = exflow.getExecutionOptions().getFailureEmails();
        List<String> emails = new ArrayList<String>();
        List<String> phoneNumbers = new ArrayList<String>();
        classify(strs, emails, phoneNumbers);
        sendMessage(phoneNumbers, message);
        try {
            exflow.getExecutionOptions().setFailureEmails(emails);
            super.alertOnFirstError(exflow);
        } finally {
            exflow.getExecutionOptions().setFailureEmails(strs);
        }
    }

    @Override
    public void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception {
        logger.info("try to alert on sla " + slaMessage);
        String slaSmsMessage = removeTagsPat.matcher(slaMessage).replaceAll("");
        String message = "[sla error]" + slaSmsMessage;
        List<String> imList = (List<String>) slaOption.getInfo().get(SlaOption.INFO_EMAIL_LIST);
        List<String> emails = new ArrayList<String>();
        List<String> phoneNumbers = new ArrayList<String>();
        classify(imList, emails, phoneNumbers);
        sendMessage(phoneNumbers, message);
        try {
            imList.removeAll(phoneNumbers);
            super.alertOnSla(slaOption, slaMessage);
        } finally {
            imList.addAll(phoneNumbers);
        }
    }

    public static void main(String[] args) throws IOException {
        Props props = new Props();
        props.put(DEFAULT_PHONE_NAME, "15010289234,13488887567,15210330871,15011413875");
        props.put(GSM_HOST_PORT, 7001);
        props.put(GSM_HOST_IP, "10.111.0.12");
//        SMSAlerter alerter = new SMSAlerter(props);
        List<String> configs = new ArrayList<String>();
        configs.add("15010289234");
        configs.add("sunshangchun@yidian-inc.com");
        List<String> emails = new ArrayList<String>();
        List<String> phoneNumbers = new ArrayList<String>();
        SMSAlerter.classify(configs, emails, phoneNumbers);
//        alerter.alertOnError();

//        alerter.sendMessage(emails, "test 测试send email");
    }
}
