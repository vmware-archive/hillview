package org.hillview.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets up the logging.
 */
public class HillviewLogging {
    public static final Logger logger;
    public static final String logFile = "hillview.log";

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.logFile", logFile);
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");

        logger = LoggerFactory.getLogger("Hillview");
        logger.info("Initialized Hillview logging");
    }
}
