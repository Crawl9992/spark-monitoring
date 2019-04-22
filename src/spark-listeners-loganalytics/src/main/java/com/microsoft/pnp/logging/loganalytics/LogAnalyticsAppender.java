package com.microsoft.pnp.logging.loganalytics;

import com.microsoft.pnp.LogAnalyticsEnvironment;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsClient;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsSendBufferClient;
import com.microsoft.pnp.logging.JSONLayout;
import com.microsoft.pnp.logging.SparkPropertyEnricher;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;

import static com.microsoft.pnp.logging.JSONLayout.TIMESTAMP_FIELD_NAME;

public class LogAnalyticsAppender extends AppenderSkeleton {
//    // SparkContext
//    // This is a copy of SparkContext.DRIVER_IDENTIFIER, which is private[spark]
//    private static final String DRIVER_IDENTIFIER = "driver";
//
//    // Spark Configuration
//    // NOTE - In Spark versions > 2.4.0, many settings have been, or will likely be, replaced with values
//    // in the internal config package, so these should be replaced with those.  Unfortunately,
//    // they are private[spark], and in Scala, so we'll need to revisit this later.
//    private static final String EXECUTOR_ID = "spark.executor.id";
//    private static final String APPLICATION_ID = "spark.app.id";
//
//    // Databricks-specific
//    private static final String DB_CLUSTER_ID = "spark.databricks.clusterUsageTags.clusterId";
//    private static final String DB_CLUSTER_NAME = "spark.databricks.clusterUsageTags.clusterName";
//    // This is the environment variable name set in our init script.
//    private static final String DB_CLUSTER_ID_ENVIRONMENT_VARIABLE = "DB_CLUSTER_ID";

    private static final Filter ORG_APACHE_HTTP_FILTER = new Filter() {
        @Override
        public int decide(LoggingEvent loggingEvent) {
            if (loggingEvent.getLoggerName().startsWith("org.apache.http")) {
                return Filter.DENY;
            }

            return Filter.NEUTRAL;
        }
    };

//    private static final Filter ADDITIONAL_FIELDS_FILTER = new Filter() {
//        @Override
//        public int decide(LoggingEvent loggingEvent) {
//            // This always returns Filter.NEUTRAL
//            // This is not how we should really do this since we aren't filtering,
//            // but because Spark uses the log4j.properties configuration, we are
//            // limited in what we can do.
//            loggingEvent.setProperty("mutated", "Mutated in a filter!");
//
//            return Filter.NEUTRAL;
//        }
//    };

    private static final String DEFAULT_LOG_TYPE = "SparkLoggingEvent";
    // We will default to environment so the properties file can override
    private String workspaceId = LogAnalyticsEnvironment.getWorkspaceId();
    private String secret = LogAnalyticsEnvironment.getWorkspaceKey();
    private String logType = DEFAULT_LOG_TYPE;
    private LogAnalyticsSendBufferClient client;

    public LogAnalyticsAppender() {
        this.addFilter(ORG_APACHE_HTTP_FILTER);
        //this.addFilter(new SparkPropertyEnricher());
        // Add a default layout so we can simplify config
        this.setLayout(new JSONLayout());
    }

    @Override
    public void activateOptions() {
        this.client = new LogAnalyticsSendBufferClient(
                new LogAnalyticsClient(this.workspaceId, this.secret),
                this.logType
        );
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
//            // There are some things that are unavailable until a certain point
//            // in the Spark lifecycle on the driver.  We will try to get as much as we can.
//            String executorId = null;
//            String applicationId = null;
//            String clusterId = null;
//            String clusterName = null;
//            SparkEnv env = SparkEnv.get();
//            if (env != null) {
//                // We should be able to get everything there
//                SparkConf conf = env.conf();
//                executorId = conf.get(EXECUTOR_ID, null);
//                // This may or may not be set in the driver when we get first get logs, but will eventually
//                // have a value.
//                applicationId = conf.get(APPLICATION_ID, null);
//                clusterId = conf.get(DB_CLUSTER_ID, null);
//                clusterName = conf.get(DB_CLUSTER_NAME, null);
//            } else {
//                // If we don't have an environment, we are probably really early
//                // on the driver, so let's see if we can get at least the cluster id
//                executorId = DRIVER_IDENTIFIER;
//                clusterId = System.getenv(DB_CLUSTER_ID_ENVIRONMENT_VARIABLE);
//            }
//
//            if (applicationId != null) {
//                loggingEvent.setProperty("applicationId", applicationId);
//            }
//
//            if (executorId != null) {
//                loggingEvent.setProperty("executorId", executorId);
//            }
//
//            if (clusterId != null) {
//                loggingEvent.setProperty("clusterId", clusterId);
//            }
//
//            if (clusterName != null) {
//                loggingEvent.setProperty("clusterName", clusterName);
//            }

            String json = this.getLayout().format(loggingEvent);
            this.client.sendMessage(json, TIMESTAMP_FIELD_NAME);
        } catch (Exception ex) {
            LogLog.error("Error sending logging event to Log Analytics", ex);
        }
    }

    @Override
    public boolean requiresLayout() {
        // We will set this to false so we can simplify our config
        // If no layout is provided, we will get the default.
        return false;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public void setLayout(Layout layout) {
        // This will allow us to configure the layout from properties to add custom JSON stuff.
        if (!(layout instanceof JSONLayout)) {
            throw new UnsupportedOperationException("layout must be an instance of JSONLayout");
        }

        super.setLayout(layout);
    }

    @Override
    public void clearFilters() {
        super.clearFilters();
        // We need to make sure to add the filter back so we don't get stuck in a loop
        this.addFilter(ORG_APACHE_HTTP_FILTER);
    }

    public String getWorkspaceId() {
        return this.workspaceId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getSecret() {
        return this.secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getLogType() {
        return this.logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }
}
