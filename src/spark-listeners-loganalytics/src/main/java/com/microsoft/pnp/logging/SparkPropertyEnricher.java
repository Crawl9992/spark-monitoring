package com.microsoft.pnp.logging;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;

public class SparkPropertyEnricher extends Filter {
    // SparkContext
    // This is a copy of SparkContext.DRIVER_IDENTIFIER, which is private[spark]
    private static final String DRIVER_IDENTIFIER = "driver";

    // Spark Configuration
    // NOTE - In Spark versions > 2.4.0, many settings have been, or will likely be, replaced with values
    // in the internal config package, so these should be replaced with those.  Unfortunately,
    // they are private[spark], and in Scala, so we'll need to revisit this later.
    private static final String EXECUTOR_ID = "spark.executor.id";
    private static final String APPLICATION_ID = "spark.app.id";

    // Databricks-specific
    private static final String DB_CLUSTER_ID = "spark.databricks.clusterUsageTags.clusterId";
    private static final String DB_CLUSTER_NAME = "spark.databricks.clusterUsageTags.clusterName";
    // This is the environment variable name set in our init script.
    private static final String DB_CLUSTER_ID_ENVIRONMENT_VARIABLE = "DB_CLUSTER_ID";

    @Override
    public int decide(LoggingEvent loggingEvent) {
        // This is not how we should really do this since we aren't actually filtering,
        // but because Spark uses the log4j.properties configuration instead of the XML
        // configuration, our options are limited.

        // There are some things that are unavailable until a certain point
        // in the Spark lifecycle on the driver.  We will try to get as much as we can.
        String executorId = null;
        String applicationId = null;
        String clusterId = null;
        String clusterName = null;
        SparkEnv env = SparkEnv.get();
        if (env != null) {
            // We should be able to get everything there
            SparkConf conf = env.conf();
            executorId = conf.get(EXECUTOR_ID, null);
            // This may or may not be set in the driver when we get first get logs, but will eventually
            // have a value.
            applicationId = conf.get(APPLICATION_ID, null);
            clusterId = conf.get(DB_CLUSTER_ID, null);
            clusterName = conf.get(DB_CLUSTER_NAME, null);
        } else {
            // If we don't have an environment, we are probably really early
            // on the driver, so let's see if we can get at least the cluster id
            executorId = DRIVER_IDENTIFIER;
            clusterId = System.getenv(DB_CLUSTER_ID_ENVIRONMENT_VARIABLE);
        }

        if (applicationId != null) {
            loggingEvent.setProperty("applicationId", applicationId);
        }

        if (executorId != null) {
            loggingEvent.setProperty("executorId", executorId);
        }

        if (clusterId != null) {
            loggingEvent.setProperty("clusterId", clusterId);
        }

        if (clusterName != null) {
            loggingEvent.setProperty("clusterName", clusterName);
        }

        return Filter.NEUTRAL;
    }
}
