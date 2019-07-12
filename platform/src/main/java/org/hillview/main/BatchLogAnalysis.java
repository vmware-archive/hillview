package org.hillview.main;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.ExtractValueFromKeyMap;
import org.hillview.maps.FindFilesMapper;
import org.hillview.maps.LoadFilesMapper;
import org.hillview.sketches.*;
import org.hillview.storage.FileSetDescription;
import org.hillview.storage.IFileReference;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.*;

/**
 * This class is used for generating Timestamp-errorCode heatmaps data files from syslogs of bugs.
 * The inputs are syslog files of format RFC5424. Rows in syslog files have a field called "errorCode" in the "StructuredData" column.
 * It extracts value of "errorCode" from "StructuredData" as a new column, to generate heatmaps of "Timestamp" vs. "errorCode".
 * Each bucket in the heatmap is the count of occurrence of some errorCode in some timestamp interval among syslogs.
 * The default number of total timestamp intervals for each bug (each heatmap) is 50.
 * The heatmap data for each bug is saved as a CSV file of columns "Timestamp", "errorCode" and "Count", where each row represents one bucket in the heatmap.
 */
public class BatchLogAnalysis {

    private static class HeatmapData {
        long[][] matrix;
        JsonList<String> errorCodeLabels = new JsonList<>();
        ArrayList<String> timeLabels = new ArrayList<>();
    }

    /**
     * This method generates the HeatmapData of "Timestamp" vs. "errorCode" for a given set of syslogs.
     * @param desc description of files of RFC5424 log format and specific file names (paths) pattern
     * @param numOfTimestampBuckets number of timestamp (x-axis) intervals for each heatmap
     * @return heatmapData including count in each bucket in the heatmap, y-tick-labels errorCodeLabels, and x-tick-labels timeLabels.
     */
    private static HeatmapData heatmapErrTime(FileSetDescription desc, int numOfTimestampBuckets) {
        /* Preprocess file desc and extract "errorCode" from "StructuredData" as a new column */
        Empty e = new Empty();
        LocalDataSet<Empty> local = new LocalDataSet<Empty>(e);
        IMap<Empty, List<IFileReference>> finder = new FindFilesMapper(desc);
        IDataSet<IFileReference> found = local.blockingFlatMap(finder);
        IMap<IFileReference, ITable> loader = new LoadFilesMapper();
        IDataSet<ITable> table = found.blockingMap(loader);
        ExtractValueFromKeyMap evkm = new ExtractValueFromKeyMap("errorCode", "StructuredData", "errorCode", -1);
        IDataSet<ITable> table1 = table.blockingMap(evkm);

        /* Find Timestamp (x-axis) buckets for the heatmap */
        DoubleDataRangeSketch rangeSketch = new DoubleDataRangeSketch("Timestamp");
        DataRange dataRange = table1.blockingSketch(rangeSketch);
        DoubleHistogramBuckets bucketsTimestamp = new DoubleHistogramBuckets(dataRange.min, dataRange.max, numOfTimestampBuckets);

        /* Find errorCode (y-axis) buckets for the heatmap */
        SampleDistinctElementsSketch sampleSketch = new SampleDistinctElementsSketch("errorCode", 0, 200);
        MinKSet<String> samples = table1.blockingSketch(sampleSketch);
        JsonList<String> leftBoundaries = samples.getLeftBoundaries(200);
        StringHistogramBuckets bucketsErrorCode = new StringHistogramBuckets(leftBoundaries.toArray(new String[0]));

        /* Generate heatmap based on Timestamp buckets and errorCode buckets, and get the count in each bucket */
        HeatmapSketch heatmapSketch = new HeatmapSketch(bucketsTimestamp, bucketsErrorCode, "Timestamp", "errorCode", 1.0, 0);
        Heatmap heatmap = table1.blockingSketch(heatmapSketch);
        HeatmapData heatmapData = new HeatmapData();
        int numOfBucketsD1 = heatmap.getNumOfBucketsD1();
        int numOfBucketsD2 = heatmap.getNumOfBucketsD2();
        heatmapData.matrix = new long[numOfBucketsD1][numOfBucketsD2];
        for (int i = 0; i<numOfBucketsD1; i++){
            for (int j = 0; j<numOfBucketsD2; j++){
                heatmapData.matrix[i][j] = heatmap.getCount(i, j);
            }
        }

        /* Get y-tick-labels errorCodeLabels and x-tick-labels timeLabels */
        heatmapData.errorCodeLabels = leftBoundaries;
        for (int x = 0; x < numOfTimestampBuckets; x++){    // save the start time only of each bucket
            double time = dataRange.min + x * (dataRange.max - dataRange.min) / numOfTimestampBuckets;
            Instant instantTime = Converters.toDate(time);
            String stringDate = Converters.toString(instantTime);
            heatmapData.timeLabels.add(stringDate);
        }
        return heatmapData;
    }

    /**
     * This method saves the heatmapData as a CSV file.
     * The file has columns "Timestamp", "errorCode" and "Count", where each row represents one bucket in the heatmap.
     * @param heatmapData includes count in each bucket, y-tick-labels errorCodeLabels and x-tick-labels timeLabels
     * @param filepath destination CSV file path
     */
    private static void saveHeatmapToFile(HeatmapData heatmapData, String filepath) {
        try (PrintWriter writer = new PrintWriter(new File(filepath))) {
            StringBuilder sb = new StringBuilder();
            sb.append("Timestamp");
            sb.append(',');
            sb.append("errorCode");
            sb.append(',');
            sb.append("Count");
            sb.append('\n');
            for (int x = 0; x < heatmapData.matrix.length; x++) {
                for (int y = 0; y < heatmapData.matrix[0].length; y++) {
                    if (heatmapData.matrix[x][y] >= 0) {        // keep zeros or not
                        sb.append(heatmapData.timeLabels.get(x));
                        sb.append(',');
                        sb.append(heatmapData.errorCodeLabels.get(y).replaceAll(",",""));
                        sb.append(',');
                        sb.append(heatmapData.matrix[x][y]);
                        sb.append('\n');
                    }
                }
            }
            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * This method generates and saves heatmapData to CSV files given input syslogs.
     * @param desc description of files of RFC5424 log format
     * @param logDir source directory of syslogs for all bugs
     * @param figDir destination directory to save the CSV files for all bugs
     * @param numOfTimestampBuckets number of timestamp (x-axis) intervals for each heatmap
     */
    public static void getBugHeatmaps(FileSetDescription desc, String logDir, String figDir, int numOfTimestampBuckets) {
        File path = new File(logDir);
        String[] bugIDs = path.list(new FilenameFilter() {    // each subDir in logDir corresponds to one bug
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        for (int i = 0; i < bugIDs.length; i++) {
            File bugFolder = new File(logDir + "/" + bugIDs[i]);
            String[] subFolders = bugFolder.list(new FilenameFilter() {
                @Override
                public boolean accept(File current, String name) {
                    return new File(current, name).isDirectory();
                }
            });
            if (subFolders.length != 0) {      // exists syslogs for that bug
                desc.fileNamePattern = "";
                if (subFolders.length == 1) {
                    desc.fileNamePattern = logDir + "/" + bugIDs[i] + "/" + subFolders[0] + "/var/log/syslog*";
                }
                else {
                    for (int j = 0; j < subFolders.length - 1; j++) {
                        desc.fileNamePattern += logDir + "/" + bugIDs[i] + "/" + subFolders[j] + "/var/log/syslog*,";
                    }
                    desc.fileNamePattern += logDir + "/" + bugIDs[i] + "/" + subFolders[subFolders.length - 1] + "/var/log/syslog*";
                }
                String filePathStr = figDir + "/" + "Bug" + bugIDs[i] + ".csv";
                Path filePath = Paths.get(filePathStr);
                if (Files.notExists(filePath)){    // call the two step methods
                    HeatmapData heatmapData = heatmapErrTime(desc, numOfTimestampBuckets);
                    saveHeatmapToFile(heatmapData, filePathStr);
                }
            }
        }
    }

    public static void main(String[] args) {
        FileSetDescription desc = new FileSetDescription();
        desc.fileKind = "genericlog";
        desc.logFormat = "%{RFC5424}";
        desc.headerRow = false;

        /* Arguments parser for two arguments: logDir and figDir */
        Options options = new Options();
        options.addOption("help",false,"java BatchLogAnalysis [-l] logDir [-f] figDir");
        Option l = OptionBuilder.withArgName( "logDir" )
                .isRequired()
                .hasArg()
                .withDescription("directory of syslog")
                .create('l');
        options.addOption(l);
        Option f = OptionBuilder.withArgName( "figDir" )
                .isRequired()
                .hasArg()
                .withDescription("directory to store the figures")
                .create('f');
        options.addOption(f);
        String logDir = "";
        String figDir = "";

        if (args.length > 0) {
            try {
                CommandLineParser parser = new GnuParser();
                CommandLine cmd = parser.parse(options,args);
                if(cmd.hasOption("help")) {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp( "java BatchLogAnalysis [-l] logDir [-f] figDir", options);
                    return;
                }
                if(cmd.hasOption("l")) {
                    logDir = cmd.getOptionValue("l");
                }
                if(cmd.hasOption("f")) {
                    figDir = cmd.getOptionValue("f");
                }
                getBugHeatmaps(desc, logDir, figDir, 50);    // default number of timestamp buckets is 50
            } catch (ParseException err) {
                System.err.println("java BatchLogAnalysis [-l] logDir [-f] figDir");
                System.exit(1);
            }
        }
        else {
            System.out.println("Please input arguments: java BatchLogAnalysis [-l] logDir [-f] figDir");
            System.exit(1);
        }
    }
}

