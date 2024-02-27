package edu.ucr.dblab.sdcel.kdtree;

public class RDDSampleUtils {
    public static int getSampleNumbers(int numPartitions, long totalNumberOfRecords, int givenSampleNumbers)
    {
        if (givenSampleNumbers > 0) {
            if (givenSampleNumbers > totalNumberOfRecords) {
                throw new IllegalArgumentException("[GeoSpark] Number of samples " + givenSampleNumbers + " cannot be larger than total records num " + totalNumberOfRecords);
            }
            return givenSampleNumbers;
        }

        // Make sure that number of records >= 2 * number of partitions
        if (totalNumberOfRecords < 2L * numPartitions) {
            throw new IllegalArgumentException("[GeoSpark] Number of partitions " + numPartitions + " cannot be larger than half of total records num " + totalNumberOfRecords);
        }

        if (totalNumberOfRecords < 1000) {
            return (int) totalNumberOfRecords;
        }

        final int minSampleCnt = numPartitions * 2;
        return (int) Math.max(minSampleCnt, Math.min(totalNumberOfRecords / 100, Integer.MAX_VALUE));
    }
}
