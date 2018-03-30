package org.hillview.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Custom thread pools
 */
public class ExecutorUtils {
    @Nullable
    private static ExecutorService computeExecutorService = null;

    // This machinery is used to create a separate thread to handle unsubscriptions.
    // If we don't do this unsubscriptions are queued behind on the thread that
    // performs the computations, and they are executed too late.
    // See https://stackoverflow.com/questions/40496565/what-thread-is-unsubscribeon-called-should-we-call-it
    private static final ExecutorService unsubExecutor =
            ExecutorUtils.newNamedThreadPool("unsub", 1, Thread.MAX_PRIORITY);
    private static final Scheduler unsubScheduler = Schedulers.from(unsubExecutor);

    /**
     * Thread pool with standard threads.
     * @param poolName  Pattern to use for the thread names.
     * @param priority  If non-negative it is used to set the thread priority.
     *                  If negative it is ignored.
     * @param numThreads Number of threads in the pool.
     */
    public static ExecutorService newNamedThreadPool(
            final String poolName, final int numThreads, int priority) {
        return Executors.newFixedThreadPool(numThreads,
                newNamedThreadFactory(poolName, true, priority));
    }

    /**
     * Default netty thread with faster access to thread local storage.
     */
    public static ExecutorService newNamedFastLocalThreadPool(
            final String poolName, final int numThreads) {
        return Executors.newFixedThreadPool(numThreads, newFastLocalThreadFactory(poolName));
    }

    /**
     * Executors and ELGs that interact with Netty benefit from FastThreadLocalThreads,
     * and therefore use Netty's DefaultThreadFactory.
     */
    public static DefaultThreadFactory newFastLocalThreadFactory(final String poolName) {
        return new DefaultThreadFactory(poolName, true);
    }

    /**
     * Standard threads with an exception handler.
     * @param poolName  Pattern to use for the thread names.
     * @param daemon    If true threads are marked as daemons.
     * @param priority  If non-negative it is used to set the thread priority.
     *                  If negative it is ignored.
     * @return          A new thread factory.
     */
    private static ThreadFactory newNamedThreadFactory(
            final String poolName, boolean daemon, int priority) {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
                .setNameFormat(poolName + "-%d")
                .setDaemon(daemon);
        if (priority >= 0)
            builder.setPriority(priority);
        return builder.build();
    }

    /**
     * Use for all compute-heavy tasks.
     */
    public static synchronized ExecutorService getComputeExecutorService() {
        if (computeExecutorService == null) {
            int cpuCount = Runtime.getRuntime().availableProcessors();
            HillviewLogger.instance.info("Detect CPUs", "Using {0} processors", cpuCount);
            computeExecutorService = newNamedThreadPool("computation", cpuCount, -1);
        }
        return computeExecutorService;
    }

    public static Scheduler getUnsubscribeScheduler() {
        return unsubScheduler;
    }
}
