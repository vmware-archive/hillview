package org.hillview.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Custom thread pools
 */
public class ExecutorUtils {
    @Nullable
    private static ExecutorService computeExecutorService = null;

    /**
     * Thread pool with standard threads
     */
    public static ExecutorService newNamedThreadPool(final String poolName, final int numThreads) {
        return Executors.newFixedThreadPool(numThreads, newNamedThreadFactory(poolName));
    }

    /**
     * Default netty thread with faster access to thread local storage
     */
    public static ExecutorService newNamedFastLocalThreadPool(final String poolName, final int numThreads) {
        return Executors.newFixedThreadPool(numThreads, newFastLocalThreadFactory(poolName));
    }

    /**
     * Executors and ELGs that interact with Netty benefit from FastThreadLocalThreads, and therefore
     * use Netty's DefaultThreadFactory.
     */
    private static DefaultThreadFactory newFastLocalThreadFactory(final String poolName) {
        return new DefaultThreadFactory(poolName, true);
    }

    /**
     * Standard threads with an exception handler.
     */
    private static ThreadFactory newNamedThreadFactory(final String poolName) {
        return new ThreadFactoryBuilder()
                .setNameFormat(poolName + "-%d")
                .setDaemon(true)
                .build();
    }

    /**
     * Use for all compute heavy tasks. For the time being, only used by LocalDataSet if it uses
     * a separate thread for compute.
     */
    public static synchronized ExecutorService getComputeExecutorService() {
        if (computeExecutorService == null) {
            int cpuCount = Runtime.getRuntime().availableProcessors();
            HillviewLogger.instance.info("Detect CPUs", "Using {0} processors", cpuCount);
            computeExecutorService = ExecutorUtils.newNamedThreadPool("computation", cpuCount);
        }
        return computeExecutorService;
    }
}
