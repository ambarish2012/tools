package com.company;

import org.apache.http.client.fluent.Request;

import java.io.Console;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Formatter;
import java.util.logging.*;

import org.json.*;

public class Main {

    private static HashSet<String> privateBuiltAccountIds = new HashSet<>();
    private static HashSet<String> privateNonBuiltAccountIds = new HashSet<>();
    private static HashMap<String, String> projectIds = new HashMap<>();
    private static HashMap<String, String> accountEmailMap = new HashMap<>();
    private static HashMap<String, Integer> accountOtherStatusCodeMap = new HashMap<>();

    private static HashSet<String> successfulBuildsAccountIds = new HashSet<>();
    private static HashSet<String> failedBuildsAccountIds = new HashSet<>();
    private static HashSet<String> hittingQuotaAccountIds = new HashSet<>();
    private static HashSet<String> unstableQuotaAccountIds = new HashSet<>();
    private static HashSet<String> noYmlFoundAccountIds = new HashSet<>();

    private final static Logger logger = Logger.getLogger(Main.class.getName());
    private static FileHandler fh = null;
    private static String apiToken;


    public static void main(String[] args) throws ParseException {
        // write your code here
        apiToken = args[0];
        init();

        List<String> accountIds = getNewAccounts(1);
        getProjectMetaData(accountIds);

        logger.log(Level.INFO, "All Account Ids: " + accountIds.toString());

        getRunStatus(privateBuiltAccountIds);

        logger.log(Level.SEVERE, "Total number of account ids = " + accountIds.size());
        logger.log(Level.SEVERE, "Num non-built account ids = " + privateNonBuiltAccountIds.size());
        logger.log(Level.SEVERE, "Num built account ids = " + privateBuiltAccountIds.size());

        logger.log(Level.SEVERE, "All built account ids = " + privateBuiltAccountIds.toString());
        logger.log(Level.SEVERE, "All non built account ids = " + privateNonBuiltAccountIds.toString());

        logger.log(Level.SEVERE, "Num failure builds account ids = " + failedBuildsAccountIds.size());
        logger.log(Level.SEVERE, "Num successful builds account ids = " + successfulBuildsAccountIds.size());
        logger.log(Level.SEVERE, "Num other status code builds account ids = " + accountOtherStatusCodeMap.size());

        /*
        for (String accountId : failedBuildsAccountIds) {
            logger.log(Level.INFO, "Console Logs for account Id: "
                    + accountId
                    + " email Id : " + accountEmailMap.get(accountId) + " "
                    + dumpConsoleLogsForFailedBuilds(accountId));
        }*/

        logger.log(Level.SEVERE, "Num no YML found account ids = " + noYmlFoundAccountIds.size());

        HashSet<String> set1 = getEmailIds(failedBuildsAccountIds);
        HashSet<String> set2 = getEmailIds(unstableQuotaAccountIds);
        HashSet<String> set3 = getEmailIds(privateNonBuiltAccountIds);
        HashSet<String> set4 = getEmailIds(privateBuiltAccountIds);

        LogEmails("Emails for failed builds AccountIds: ", set1);
        LogEmails("Emails for unstable Quota AccountIds: ", set2);
        LogEmails("Emails for no build runs AccountIds: ", set3);
        LogEmails("Emails for all private built AccountIds: ", set4);
        // LogEmails("Emails for no yml found account ids: ", getEmailIds(noYmlFoundAccountIds));


        for (String accountId : successfulBuildsAccountIds) {
            logger.log(Level.SEVERE, "Runs for email Id = " + accountEmailMap.get(accountId));
            anySuccessfulRuns(accountId);
        }

        for (String accountId : failedBuildsAccountIds) {
            logger.log(Level.SEVERE, "Runs for email Id = " + accountEmailMap.get(accountId));
            anySuccessfulRuns(accountId);
        }

        for (String accountId : failedBuildsAccountIds) {
            logger.log(Level.INFO, "Console Logs for account Id: "
                    + accountId
                    + " email Id : " + accountEmailMap.get(accountId) + " "
                    + dumpConsoleLogsForFailedBuilds(accountId));
        }
    }

    public static String dumpConsoleLogsForFailedBuilds(String accountId) {
        // get all subscriptions
        String url = "https://api.shippable.com/subscriptionAccounts?accountIds=" + accountId;
        String json = makeGetRestCall(url);

        HashSet<String> subscriptionIds = new HashSet<>();
        JSONArray jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);
            subscriptionIds.add(jsonObject.getString("subscriptionId"));
        }

        // get all runs
        StringBuilder runUrl = new StringBuilder("https://api.shippable.com/runs?subscriptionIds=");
        boolean first = true;
        for (String subscription : subscriptionIds) {
            if (!first) {
                runUrl.append(",");
            }

            runUrl.append(subscription);
            first = false;
        }
        json = makeGetRestCall(runUrl.toString());

        HashSet<String> runs = new HashSet<>();
        jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);
            runs.add(jsonObject.getString("id"));
        }

        // get all jobs
        StringBuilder jobsUrl = new StringBuilder("https://api.shippable.com/jobs?runIds=");
        first = true;
        for (String run : runs) {
            if (!first) {
                jobsUrl.append(",");
            }

            jobsUrl.append(run);
            first = false;
        }
        json = makeGetRestCall(jobsUrl.toString());

        HashSet<String> jobs = new HashSet<>();
        jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);
            jobs.add(jsonObject.getString("id"));
        }

        // get all jobConsoles
        StringBuilder consoleLogs = new StringBuilder();
        for(String job : jobs) {
            String jobConsolesUrl = String.format("https://api.shippable.com/jobs/%s/consoles?download=true", job);
            String consoleLog = makeGetRestCall(jobConsolesUrl);

            if ((consoleLog != null) && (!consoleLog.isEmpty())) {
                if ((consoleLog.indexOf("failed to find shippable yml") != -1) ||
                        (consoleLog.indexOf("failed to find yml file") != -1)) {
                    noYmlFoundAccountIds.add(accountId);
                }

                consoleLogs.append(consoleLog);
                consoleLogs.append(System.getProperty("line.separator"));
            }
        }

        return consoleLogs.toString();
    }

    public static void init(){
        try {
            fh=new FileHandler("/Users/ambarish/Desktop/loggerExample.log", false);
        } catch (SecurityException | IOException e) {
            e.printStackTrace();
        }

        fh.setFormatter(new java.util.logging.Formatter() {
            @Override
            public String format(LogRecord rec) {
                StringBuffer buf = new StringBuffer();
                buf.append(new java.util.Date());
                buf.append(' ');
                buf.append(rec.getLevel());
                buf.append(' ');
                buf.append(rec.getMessage());
                buf.append(System.getProperty("line.separator"));
                buf.append(System.getProperty("line.separator"));
                return buf.toString();
            }
        });

        logger.addHandler(fh);
        logger.setLevel(Level.INFO);
    }

    public static void LogEmails(String msg, HashSet<String> emailIds) {
        if ((emailIds == null) || emailIds.isEmpty())
            return;

        System.out.println(emailIds);
        logger.log(Level.SEVERE, msg + emailIds.toString());
    }

    public static HashSet<String> getEmailIds(HashSet<String> accountIds) {
        if (accountIds.isEmpty())
            return null;

        StringBuffer url = new StringBuffer("https://api.shippable.com/accountProfiles/?accountIds=");

        boolean first = true;
        for (String accounId : accountIds) {
            if (!first) {
                url.append(",");
            }

            url.append(accounId);
            first = false;
        }

        String json = makeGetRestCall(url.toString());
        JSONArray jsonArray = new JSONArray(json);

        HashSet<String> emailIds = new HashSet<>();
        for(int index = 0; index < jsonArray.length(); index++) {
            String emailId = jsonArray.getJSONObject(index).getString("defaultEmailId");
            String accountId = jsonArray.getJSONObject(index).getString("accountId");

            emailIds.add(emailId);
            accountEmailMap.put(accountId, emailId);
        }

        return emailIds;
    }

    public static boolean anySuccessfulRuns(String accountId) {
        boolean successfulRun = false;

        // get all subscription ids
        StringBuilder url = new StringBuilder("https://api.shippable.com/subscriptionAccounts?accountIds=");
        url.append(accountId);

        String json = makeGetRestCall(url.toString());
        HashSet<String> subscriptionIds = new HashSet<>();

        // extract all subscription ids
        JSONArray jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);
            subscriptionIds.add(jsonObject.getString("subscriptionId"));
        }

        // get all runs with status code 30 or 80
        url = new StringBuilder("https://api.shippable.com/runs?subscriptionIds=");

        boolean first = true;
        for (String subscriptionId : subscriptionIds) {
            if (!first) {
                url.append(",");
            }

            url.append(subscriptionId);
            first = false;
        }

        json = makeGetRestCall(url.toString());
        jsonArr = new JSONArray(json);
        HashSet<String> status30 = new HashSet<>();
        HashSet<String> status80 = new HashSet<>();

        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);

            if (jsonObject.getBoolean("isPrivate")) {
                int statusCode = jsonObject.getInt("statusCode");
                String projectId = jsonObject.getString("projectId");
                if (statusCode == 30 || statusCode == 50 || statusCode == 40) {
                    status30.add(projectId);
                } else if (statusCode == 80) {
                    status80.add(projectId);
                }
            }
        }

        logger.log(Level.SEVERE, "Successful projects = " + status30.size());
        logger.log(Level.SEVERE, "Failed projects = " + status80.size());

        return successfulRun;
    }

    public static void getRunStatus(HashSet<String> accountIds) {
        String url = "https://api.shippable.com/projects/%s/branchRunStatus";

        for (String accounId : accountIds) {
            String json = makeGetRestCall(String.format(url, projectIds.get(accounId)));

            JSONArray jsonArr = new JSONArray(json);
            for (int index = 0; index < jsonArr.length(); index++) {
                JSONObject jsonObject = jsonArr.getJSONObject(index);
                int statusCode = jsonObject.getInt("statusCode");

                switch (statusCode)
                {
                    case 30:
                        unstableQuotaAccountIds.remove((accounId));
                        failedBuildsAccountIds.remove(accounId);
                        successfulBuildsAccountIds.add(accounId);
                        break;
                    case 40:
                        hittingQuotaAccountIds.add(accounId);
                        break;
                    case 50:
                        // unstable
                        unstableQuotaAccountIds.add(accounId);
                        break;
                    case 80:
                        failedBuildsAccountIds.add(accounId);
                        break;
                    default:
                        accountOtherStatusCodeMap.put(accounId, statusCode);
                        break;
                }
            }
        }
    }

    public static void getProjectMetaData(List<String> accountIds) {
        String url = "https://api.shippable.com/projects?projectIds=***&enabledBy=%s&autoBuild=true";

        for (String accountId: accountIds) {
            String json = makeGetRestCall(String.format(url, accountId));

            JSONArray jsonArr = new JSONArray(json);
            for (int index = 0; index < jsonArr.length(); index++) {
                JSONObject jsonObject = jsonArr.getJSONObject(index);
                if (jsonObject.getBoolean("isPrivateRepository")) {
                    projectIds.put(accountId, jsonObject.getString("id"));
                    int lastBuildGroupNumber = jsonObject.getInt("lastBuildGroupNumber");
                    System.out.println(lastBuildGroupNumber);
                    if (lastBuildGroupNumber > 0) {
                        privateBuiltAccountIds.add(accountId);
                    } else {
                        privateNonBuiltAccountIds.add(accountId);
                    }
                }
            }
        }
    }

    public static List<String> getNewAccounts(int noDaysSinceToday) throws ParseException {

        // today
        Calendar date = new GregorianCalendar();
        // reset hour, minutes, seconds and millis
        date.set(Calendar.HOUR_OF_DAY, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        Date t = date.getTime();

        List<String> accountIds = new ArrayList<String>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, noDaysSinceToday * -1);
        Date earliestDate = cal.getTime();

        String json =  makeGetRestCall("https://api.shippable.com/accountTokens?accountIds=***&isInternal=true&limit=100");
        JSONArray jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);

            SimpleDateFormat format = new SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));

            Date createdAt =  format.parse(jsonObject.getString("createdAt"));

            if (createdAt.after(earliestDate)) {
                String accountId = jsonObject.getString("accountId");
                accountIds.add(accountId);
                logger.log(Level.SEVERE, "Date of accountId " + accountId + " " + createdAt.toString());
            }
        }

        return accountIds;
    }

    public static String makeGetRestCall(String url) {
        try {
            System.out.println("Making API Call " + url);

            String response = Request.Get(url)
                    .addHeader("Authorization", "apiToken " + apiToken)
                    .execute()
                    .returnContent()
                    .asString();

            logger.log(Level.FINEST, response);
            return response;
        } catch (IOException ex) {
            logger.log(Level.SEVERE, ex.toString());
            return null;
        }
    }
}
