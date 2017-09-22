package org.hillview.utils;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Sets up the logging.
 * The 'initialize' method should be called early.
 * Singleton pattern.
 */
public class HillviewLogManager {
    public static HillviewLogManager instance = new HillviewLogManager();
    private final static String logFileName = "hillview.log";

    public final Logger logger;

    private HillviewLogManager() {
        this.logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }

    public void initialize() throws IOException {
        this.logger.setLevel(Level.INFO);
        // The following disables logging on console
        //this.logger.setUseParentHandlers(false);

        FileHandler logFile = new FileHandler(HillviewLogManager.logFileName);
        SimpleFormatter formatter = new SimpleFormatter();
        logFile.setFormatter(formatter);
        this.logger.addHandler(logFile);
    }
}
