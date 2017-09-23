package org.hillview.dataset.api;

import org.hillview.utils.HillviewLogging;

/**
 * Base class for all IDataSets.
 * @param <T>  Type of data in dataset.
 */
public abstract class BaseDataSet<T> implements IDataSet<T> {
    static int uniqueId = 0;
    protected final int id;

    public BaseDataSet() {
        this.id = BaseDataSet.uniqueId++;
    }

    @Override
    public String toString() {
        String host = "?";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            host = localMachine.getHostName();
        } catch (java.net.UnknownHostException e) {
            HillviewLogging.logger.error("Cannot get host name");
        }
        return this.getClass().getName() + "(" + this.id + ")@" + host;
    }

    /**
     * Helper function which can be invoked in a map over streams to log the processing
     * over each stream element.
     */
    protected <S> S log(S data, String message) {
        this.log(message);
        return data;
    }

    protected void log(String message) {
        HillviewLogging.logger.info(this.toString() + ":" + message);
    }
}
