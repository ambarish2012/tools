package com.company;

import com.mongodb.client.model.Indexes;
import org.apache.http.client.fluent.Request;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

import org.json.*;

public class Main {
    private static HashSet<String> privateBuiltAccountIds = new HashSet<>();
    private static HashSet<String> privateNonBuiltAccountIds = new HashSet<>();

    private static HashMap<String, HashSet<String>> accountIdsByDate = new HashMap<>();
    private static HashMap<String, HashSet<String>> privateBuiltProjectIdsByDate = new HashMap<>();
    private static HashMap<String, HashSet<String>> privateFailedBuildProjectIdsByDate = new HashMap<>();

    private static HashSet<String> failedBuildsProjectIds = new HashSet<>();
    private static HashMap<String, String> projectRunId = new HashMap<>();
    private static HashSet<String> noYmlFoundProjectIds = new HashSet<>();
    private static HashMap<String, String> projectIdYml = new HashMap<>();
    private static ConcurrentHashMap<String, String> projectAccountIdMap = new ConcurrentHashMap<String, String>();

    private static HashMap<String, String> accountProjectIdMap = new HashMap<>();
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
    private static MongoClient mongoClient = null;
    private static MongoDatabase database = null;


    public static void main(String[] args) throws ParseException {
        apiToken = args[0];
        int numDaysToTrack = Integer.parseInt(args[1]);

        init();
        connectToMongo();


        getNewAccounts(numDaysToTrack);
        getProjectMetaData();
        getRunStatus();
        detectNoYml();
        writeToMongo();

        logger.log(Level.SEVERE, "failed build project ids " + failedBuildsProjectIds.toString());
        logger.log(Level.SEVERE, "No YML project ids " + noYmlFoundProjectIds.toString());

        // detectFailedBuilds(numDaysToTrack);
    }

    private static void writeToMongo() {
        Iterator it = privateFailedBuildProjectIdsByDate.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            HashSet<String> projectIds = (HashSet<String>) pair.getValue();

            MongoCollection<Document> collection = database.getCollection((String)pair.getKey());

            collection.dropIndexes();
            collection.drop();

            collection.createIndex(Indexes.ascending("projectid"));

            for (String projectId : projectIds) {
                Document doc = new Document("projectid", projectId)
                        .append("accountid", projectAccountIdMap.get(projectId))
                        .append("lastrunid", projectRunId.get(projectId));

                if (noYmlFoundProjectIds.contains(projectId)) {
                    doc.append("noyml", true);
                }

                if (projectIdYml.containsKey(projectId)) {
                    doc.append("yml", projectIdYml.get(projectId));
                }

                collection.insertOne(doc);
            }
        }
    }

    private static void detectNoYml() {
        String url = "https://api.shippable.com/runs/%s";

        Iterator it = projectRunId.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();

            String projectId = (String) pair.getKey();
            String runId = (String) pair.getValue();

            String json = makeGetRestCall(String.format(url, runId));
            JSONObject jsonObject = new JSONObject(json);
            JSONObject yml = jsonObject.getJSONObject("cleanRunYml");

            if ((yml != null) && (yml.length() > 0)) {
                projectIdYml.putIfAbsent(projectId, yml.toString());
            } else {
                noYmlFoundProjectIds.add(projectId);
            }
        }
    }

    private static void getRunStatus() {
        String url = "https://api.shippable.com/runs?projectIds=%s";
        Iterator it = privateBuiltProjectIdsByDate.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();

            HashSet<String> builtProjectIds = (HashSet<String>) pair.getValue();
            HashSet<String> buildFailureProjectIds = new HashSet<>();

            for (String projectId : builtProjectIds) {
                String json = makeGetRestCall(String.format(url, projectId));
                String runId = null;

                JSONArray jsonArr = new JSONArray(json);
                for (int index = 0; index < jsonArr.length(); index++) {
                    JSONObject jsonObject = jsonArr.getJSONObject(index);

                    // store the first run number for yml check
                    if (runId == null) {
                        runId = jsonObject.getString("id");
                    }

                    int statusCode = jsonObject.getInt("statusCode");

                    if (statusCode == 30 || statusCode == 40) {
                        buildFailureProjectIds.remove(projectId);
                        projectRunId.remove(projectId);
                        break;
                    } else if (statusCode == 80) {
                        buildFailureProjectIds.add(projectId);
                        projectRunId.putIfAbsent(projectId, runId);
                    }
                }
            }

            if (buildFailureProjectIds.size() > 0) {
                String collectionKey = (String)pair.getKey();
                privateFailedBuildProjectIdsByDate.put(collectionKey, buildFailureProjectIds);
                failedBuildsProjectIds.addAll(buildFailureProjectIds);
            }
       }
    }

    private static void getProjectMetaData() {
        String url = "https://api.shippable.com/projects?projectIds=***&enabledBy=%s&autoBuild=true";

        Iterator it = accountIdsByDate.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());

            HashSet<String> accountIds = (HashSet<String>) pair.getValue();
            HashSet<String> builtProjectIds = new HashSet<>();

            for (String accountId: accountIds) {
                String json = makeGetRestCall(String.format(url, accountId));

                JSONArray jsonArr = new JSONArray(json);
                for (int index = 0; index < jsonArr.length(); index++) {
                    JSONObject jsonObject = jsonArr.getJSONObject(index);
                    if (jsonObject.getBoolean("isPrivateRepository")) {
                        int lastBuildGroupNumber = jsonObject.getInt("lastBuildGroupNumber");
                        System.out.println(lastBuildGroupNumber);
                        if (lastBuildGroupNumber > 0) {
                            builtProjectIds.add(jsonObject.getString("id"));
                            projectAccountIdMap.putIfAbsent(jsonObject.getString("id"), accountId);
                        }
                    }
                }
            }

            privateBuiltProjectIdsByDate.put((String)pair.getKey(), builtProjectIds);
        }
    }

    private static HashSet<String> getNewAccounts(int noDaysSinceToday) throws ParseException {
        // today
        Calendar date = new GregorianCalendar();
        // reset hour, minutes, seconds and millis
        date.set(Calendar.HOUR_OF_DAY, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        Date today = date.getTime();

        HashSet<String> accountIds = new HashSet<>();
        HashSet<String> accIdsByDate = new HashSet<>();

        String json =  makeGetRestCall("https://api.shippable.com/accountTokens?accountIds=***&isInternal=true&limit=100");
        int dayIndex = 0;

        JSONArray jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);

            String accountId = jsonObject.getString("accountId");

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date createdAt =  format.parse(jsonObject.getString("createdAt"));

            if (!createdAt.after(today)) {
                String todayStr = getFormattedDateString(today);
                accountIdsByDate.put(todayStr, new HashSet<>(accIdsByDate));

                accIdsByDate.clear();
                dayIndex++;
                if (dayIndex >= noDaysSinceToday) {
                    break;
                }

                date.add(Calendar.DAY_OF_MONTH, -1);
                today = date.getTime();
            }

            accountIds.add(accountId);
            accIdsByDate.add(accountId);
        }

        return accountIds;
    }

    public static void detectFailedBuilds(int numDays) throws ParseException{
        HashSet<String> accountIds = getNewAccounts(numDays);

        getProjectMetaData(accountIds);
        getRunStatus(privateBuiltAccountIds);

        logger.log(Level.INFO, "All Account Ids: " + accountIds.toString());
        logger.log(Level.SEVERE, "Total number of account ids = " + accountIds.size());
        logger.log(Level.SEVERE, "Num non-built account ids = " + privateNonBuiltAccountIds.size());
        logger.log(Level.SEVERE, "Num built account ids = " + privateBuiltAccountIds.size());
        logger.log(Level.SEVERE, "All built account ids = " + privateBuiltAccountIds.toString());
        logger.log(Level.SEVERE, "All non built account ids = " + privateNonBuiltAccountIds.toString());
        logger.log(Level.SEVERE, "Num failure builds account ids = " + failedBuildsAccountIds.size());
        logger.log(Level.SEVERE, "Num successful builds account ids = " + successfulBuildsAccountIds.size());
        // logger.log(Level.SEVERE, "Num other status code builds account ids = " + accountOtherStatusCodeMap.size());
        // logger.log(Level.SEVERE, "Num no YML found account ids = " + noYmlFoundAccountIds.size());

        HashSet<String> set1 = getEmailIds(failedBuildsAccountIds);
        // HashSet<String> set2 = getEmailIds(unstableQuotaAccountIds);
        HashSet<String> set3 = getEmailIds(privateNonBuiltAccountIds);
        HashSet<String> set4 = getEmailIds(privateBuiltAccountIds);

        LogEmails("Emails for failed builds AccountIds: ", set1);
        // LogEmails("Emails for unstable Quota AccountIds: ", set2);
        LogEmails("Emails for no build runs AccountIds: ", set3);
        LogEmails("Emails for all private built AccountIds: ", set4);

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        for (String accountId : successfulBuildsAccountIds) {
            executor.execute(() -> {
                anySuccessfulRuns(accountId);
            });
        }

        for (String accountId : failedBuildsAccountIds) {
            executor.execute(() -> {
                anySuccessfulRuns(accountId);
            });
        }

        for (String accountId : failedBuildsAccountIds) {
            executor.execute(() -> {
                dumpConsoleLogsForFailedBuilds(accountId);
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "failure in main " + e.getMessage());
        }
    }

    private static void anySuccessfulRuns(String accountId) {
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

        logger.log(Level.SEVERE, "Successful projects for email id = "
                + accountEmailMap.get(accountId)
                + " "
                + status30.size());

        logger.log(Level.SEVERE, "Failed projects for email id = "
                + accountEmailMap.get(accountId)
                + " "
                + status80.size());
    }

    private static void getRunStatus(HashSet<String> accountIds) {
        String url = "https://api.shippable.com/projects/%s/branchRunStatus";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        Set setFailedBuildsAccountIds = Collections.synchronizedSet(failedBuildsAccountIds);
        Set setSuccessfulBuildsAccountIds = Collections.synchronizedSet(successfulBuildsAccountIds);

        for (String accounId : accountIds) {
            executor.execute(() -> {
                String json = makeGetRestCall(String.format(url, accountProjectIdMap.get(accounId)));

                JSONArray jsonArr = new JSONArray(json);
                for (int index = 0; index < jsonArr.length(); index++) {
                    JSONObject jsonObject = jsonArr.getJSONObject(index);
                    int statusCode = jsonObject.getInt("statusCode");

                    if (statusCode == 30 || statusCode == 40) {
                        setFailedBuildsAccountIds.remove(accounId);
                        setSuccessfulBuildsAccountIds.add(accounId);
                        break;
                    } else if (statusCode == 80) {
                        setFailedBuildsAccountIds.add(accounId);
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "failure in getRunStatus " + e.getMessage());
        }
    }

    public static void getProjectMetaData(HashSet<String> accountIds) {
        String url = "https://api.shippable.com/projects?projectIds=***&enabledBy=%s&autoBuild=true";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        Set setPrivateBuiltAccountIds = Collections.synchronizedSet(privateBuiltAccountIds);
        Set setPrivateNonBuiltAccountIds = Collections.synchronizedSet(privateNonBuiltAccountIds);
        Map mapAccountProjectIdMap = Collections.synchronizedMap(accountProjectIdMap);

        for (String accountId: accountIds) {
            executor.execute(() -> {
                String json = makeGetRestCall(String.format(url, accountId));

                JSONArray jsonArr = new JSONArray(json);
                for (int index = 0; index < jsonArr.length(); index++) {
                    JSONObject jsonObject = jsonArr.getJSONObject(index);
                    if (jsonObject.getBoolean("isPrivateRepository")) {
                        mapAccountProjectIdMap.put(accountId, jsonObject.getString("id"));
                        int lastBuildGroupNumber = jsonObject.getInt("lastBuildGroupNumber");
                        System.out.println(lastBuildGroupNumber);
                        if (lastBuildGroupNumber > 0) {
                            setPrivateBuiltAccountIds.add(accountId);
                        } else {
                            setPrivateNonBuiltAccountIds.add(accountId);
                        }
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "failure in getProjectMetaData " + e.getMessage());
        }
    }

    private static String getFormattedDateString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("MM-dd-YYYY");
        return format.format(date);
    }

    private static void init(){
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

    private static void LogEmails(String msg, HashSet<String> emailIds) {
        if ((emailIds == null) || emailIds.isEmpty())
            return;

        System.out.println(emailIds);
        logger.log(Level.SEVERE, msg + emailIds.toString());
    }

    private static HashSet<String> getEmailIds(HashSet<String> accountIds) {
        if (accountIds.isEmpty())
            return null;

        StringBuilder url = new StringBuilder("https://api.shippable.com/accountProfiles/?accountIds=");

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

    private static void dumpConsoleLogsForFailedBuilds(String accountId) {
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

        logger.log(Level.INFO, "Console Logs for account Id: "
                + accountId
                + " email Id : " + accountEmailMap.get(accountId) + " "
                + consoleLogs.toString());

        return;
    }

    private static String makeGetRestCall(String url) {
        try {
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

    public static void connectToMongo() {
        mongoClient = new MongoClient( "localhost" , 27017 );
        database = mongoClient.getDatabase("buildfailures");
    }

}
