package com.company;

import com.mongodb.client.model.Indexes;
import org.apache.http.client.fluent.Request;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import org.json.*;

public class Main {
    private static HashMap<String, HashSet<String>> accountIdsByDate = new HashMap<>();
    private static Map<String, HashSet<String>> privateFailedBuildProjectIdsByDate = new HashMap<>();
    private static Map<String, HashSet<String>> privateSuccessfulBuildProjectIdsByDate = new HashMap<>();
    private static Map<String, HashSet<String>> privateNonBuiltProjectIdsByDate = new HashMap<>();

    private static Map<String, String> nonBuiltProjectIdMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> projectAccountIdMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> builtProjectDateMap = new ConcurrentHashMap<>();

    private static Set<String> failedBuildsProjectIds = ConcurrentHashMap.newKeySet();
    private static Set<String> successfulBuildsProjectIds = ConcurrentHashMap.newKeySet();

    private static ConcurrentHashMap<String, String> projectRunId = new ConcurrentHashMap<>();
    private static HashSet<String> noYmlFoundProjectIds = new HashSet<>();
    private static HashMap<String, String> projectIdYml = new HashMap<>();
    private static HashMap<String, String> accountEmailMap = new HashMap<>();

    private static Set<String> successfulBuildsAccountIds = ConcurrentHashMap.newKeySet();
    private static HashSet<String> noYmlFoundAccountIds = new HashSet<>();

    private final static Logger logger = Logger.getLogger(Main.class.getName());
    private static FileHandler fh = null;
    private static String apiToken;
    private static MongoClient mongoClient = null;
    private static MongoDatabase failedBuildsDatabase = null;
    private static MongoDatabase greenBuildsDatabase = null;
    private static MongoDatabase noBuildsDatabase = null;

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
    }

    private static void writeToMongo() {
        for (String projectId : failedBuildsProjectIds) {
            String date = builtProjectDateMap.get(projectId);
            privateFailedBuildProjectIdsByDate.computeIfAbsent(date, projectIds -> new HashSet<>()).add(projectId);
        }

        for (String projectId : successfulBuildsProjectIds) {
            String date = builtProjectDateMap.get(projectId);
            privateSuccessfulBuildProjectIdsByDate.computeIfAbsent(date, projectIds -> new HashSet<>()).add(projectId);
        }

        for (Map.Entry<String, String> entry : nonBuiltProjectIdMap.entrySet()) {
            privateNonBuiltProjectIdsByDate.computeIfAbsent(entry.getValue(), projectIds -> new HashSet<>())
                    .add(entry.getKey());
        }

        insertMongoDocumentForCollection(privateFailedBuildProjectIdsByDate, failedBuildsDatabase);
        insertMongoDocumentForCollection(privateSuccessfulBuildProjectIdsByDate, greenBuildsDatabase);
        insertMongoDocumentForCollection(privateNonBuiltProjectIdsByDate, noBuildsDatabase);
    }

    private static void insertMongoDocumentForCollection(Map<String, HashSet<String>> map, MongoDatabase database) {
        for (Map.Entry<String, HashSet<String>> entry : map.entrySet()) {
            HashSet<String> projectIds = entry.getValue();

            MongoCollection<Document> collection = database.getCollection(entry.getKey());
            if (collection.count()  == 0 ) {
                collection.createIndex(Indexes.ascending("projectid"));

                for (String projectId : projectIds) {
                    Document doc = new Document("projectid", projectId)
                            .append("accountid", projectAccountIdMap.get(projectId))
                            .append("email", getEmailId(projectAccountIdMap.get(projectId)));

                    if (noYmlFoundProjectIds.contains(projectId)) {
                        doc.append("noyml", true);
                    }

                    if (projectIdYml.containsKey(projectId)) {
                        doc.append("yml", projectIdYml.get(projectId));
                    }

                    if (projectRunId.containsKey(projectId)) {
                        doc.append("lastrunid", projectRunId.get(projectId));
                    }

                    collection.insertOne(doc);
                }
            }
        }
    }


    private static void detectNoYml() {
        String url = "https://api.shippable.com/runs/%s";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : projectRunId.entrySet()) {
            executor.execute(() -> {
                String projectId = entry.getKey();
                String runId = entry.getValue();

                String json = makeGetRestCall(String.format(url, runId));
                JSONObject jsonObject = new JSONObject(json);
                JSONObject yml = jsonObject.getJSONObject("cleanRunYml");

                if ((yml != null) && (yml.length() > 0)) {
                    projectIdYml.putIfAbsent(projectId, yml.toString());
                } else {
                    noYmlFoundProjectIds.add(projectId);
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "failure in detectNoYml " + e.getMessage());
        }
    }

    private static void getRunStatus() {
        String url = "https://api.shippable.com/runs?projectIds=%s";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        for (Map.Entry<String, String> entry : builtProjectDateMap.entrySet()) {
            executor.execute(() -> {
                String projectId = entry.getKey();
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
                        failedBuildsProjectIds.remove(projectId);
                        projectRunId.remove(projectId);

                        if (statusCode == 30) {
                            successfulBuildsProjectIds.add(projectId);
                        }

                        break;
                    } else if (statusCode == 80 || statusCode == 50) {
                        failedBuildsProjectIds.add(projectId);
                        projectRunId.put(projectId, runId);
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

    private static void getProjectMetaData() {
        String url = "https://api.shippable.com/projects?projectIds=***&enabledBy=%s&autoBuild=true";

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        for (Map.Entry<String, HashSet<String>> entry : accountIdsByDate.entrySet()) {
            String key = entry.getKey();

            if (!wasDayProcessed(key)) {
                HashSet<String> accountIds = entry.getValue();

                for (String accountId : accountIds) {
                    executor.execute(() -> {
                        String json = makeGetRestCall(String.format(url, accountId));

                        JSONArray jsonArr = new JSONArray(json);
                        for (int index = 0; index < jsonArr.length(); index++) {
                            JSONObject jsonObject = jsonArr.getJSONObject(index);
                            if (jsonObject.getBoolean("isPrivateRepository")) {
                                int lastBuildGroupNumber = jsonObject.getInt("lastBuildGroupNumber");
                                String projectId = jsonObject.getString("id");

                                if (lastBuildGroupNumber > 0) {
                                    builtProjectDateMap.put(projectId, key);
                                } else {
                                    nonBuiltProjectIdMap.put(projectId, key);
                                }

                                projectAccountIdMap.put(projectId, accountId);
                            }
                        }
                    });
                }
            } else {
                logger.log(Level.INFO, "skipping analysis for date " + key);
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "failure in getProjectMetaData " + e.getMessage());
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

        Date curentDay = date.getTime();
        date.add(Calendar.DAY_OF_MONTH, -1);
        Date previousDay = date.getTime();

        HashSet<String> accountIds = new HashSet<>();
        HashSet<String> accIdsByDate = new HashSet<>();

        StringBuilder sb = new StringBuilder("https://api.shippable.com/accountTokens?accountIds=***&isInternal=true&limit=");
        sb.append(noDaysSinceToday * 50);

        String json =  makeGetRestCall(sb.toString());
        int dayIndex = 0;

        JSONArray jsonArr = new JSONArray(json);
        for (int index = 0; index < jsonArr.length(); index++) {
            JSONObject jsonObject = jsonArr.getJSONObject(index);

            String accountId = jsonObject.getString("accountId");

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date createdAt =  format.parse(jsonObject.getString("createdAt"));

            if (createdAt.before(previousDay)) {
                String todayStr = getFormattedDateString(previousDay);
                accountIdsByDate.put(todayStr, new HashSet<>(accIdsByDate));

                accIdsByDate.clear();
                dayIndex++;
                if (dayIndex >= noDaysSinceToday) {
                    break;
                }

                curentDay = previousDay;
                date.add(Calendar.DAY_OF_MONTH, -1);
                previousDay = date.getTime();
            }

            if (createdAt.after(previousDay) && createdAt.before(curentDay)) {
                accountIds.add(accountId);
                accIdsByDate.add(accountId);
            }
        }

        return accountIds;
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

    private static String getEmailId(String accountId) {
        StringBuilder url = new StringBuilder("https://api.shippable.com/accountProfiles/?accountIds=");
        url.append(accountId);

        String json = makeGetRestCall(url.toString());
        return new JSONArray(json).getJSONObject(0).getString("defaultEmailId");
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
        failedBuildsDatabase = mongoClient.getDatabase("FailedBuildsDB");
        greenBuildsDatabase = mongoClient.getDatabase("SuccessBuildsDB");
        noBuildsDatabase = mongoClient.getDatabase("NoBuildsDB");
    }

    public static boolean wasDayProcessed(String collection) {
        return ((failedBuildsDatabase.getCollection(collection).count() != 0)
                || (greenBuildsDatabase.getCollection(collection).count() != 0))
                || (noBuildsDatabase.getCollection(collection).count() != 0);
    }
}
