package uk.ac.ic.wlgitbridge.bridge.swap.job;

import uk.ac.ic.wlgitbridge.util.Log;
import uk.ac.ic.wlgitbridge.bridge.swap.job.SwapJob.CompressionMethod;

/**
 * Created by winston on 23/08/2016.
 */
public class SwapJobConfig {

    private final int minProjects;
    private final int lowGiB;
    private final int highGiB;
    private final long intervalMillis;
    private final String compressionMethod;

    public SwapJobConfig(
            int minProjects,
            int lowGiB,
            int highGiB,
            long intervalMillis,
            String compressionMethod
    ) {
        this.minProjects = minProjects;
        this.lowGiB = lowGiB;
        this.highGiB = highGiB;
        this.intervalMillis = intervalMillis;
        this.compressionMethod = compressionMethod;
    }

    public int getMinProjects() {
        return minProjects;
    }

    public int getLowGiB() {
        return lowGiB;
    }

    public int getHighGiB() {
        return highGiB;
    }

    public long getIntervalMillis() {
        return intervalMillis;
    }

    public SwapJob.CompressionMethod getCompressionMethod() {
      CompressionMethod result = SwapJob.stringToCompressionMethod(compressionMethod);
      if (result == null) {
        Log.info("SwapJobConfig: un-supported compressionMethod '" + compressionMethod + "', default to 'bzip2'");
        result = CompressionMethod.Bzip2;
      }
      return result;
    }
}
