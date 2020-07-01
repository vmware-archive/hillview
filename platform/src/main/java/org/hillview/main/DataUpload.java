/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.main;

import javax.annotation.Nullable;

import org.apache.commons.io.FilenameUtils;
import org.hillview.management.ClusterConfig;
import org.hillview.storage.*;
import org.hillview.table.Schema;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import org.apache.commons.cli.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This entry point is only used for uploading data to a cluster. The user provides configuration details such as the
 * path to the cluster config file, the location of the file to upload etc. If a schema is not provided the program
 * scans the file to determine one. It then proceeds to read the file as a stream, and  chop it to smaller shards,
 * placing one shard in each server in a round robin fashion. The user chooses whether to upload the file in csv
 * format or in orc format. A schema file is also placed in each server.
 */
public class DataUpload {
    private static class Params {
        final int defaultChunkSize = 100000; // number of lines in default file chunk
        final String defaultSchemaName = "schema";
        @Nullable
        String inputSchemaName = null;
        String filename = ""; // the file to be sent
        String destinationFolder = ""; // the destination path where the files will be put
        @Nullable
        String cluster = null; // the path to the cluster config json file
        boolean hasHeader; // true if file has a header row (only used for csv inputs)
        boolean saveOrc; // true if saving as orc, otherwise save as csv;
        int chunkSize = defaultChunkSize; // the number of lines in each shard.
        boolean allowFewerColumns;
        @Nullable
        String grokPattern; // when parsing a log file this is the pattern expected
        int skipLines;  // number of lines to skip from the beginning
    }

    private void usage(Options options) {
        final String usageString = "Usage: DataUpload ";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(usageString, options);
    }

    /**
     * Parses the command line and fills up the parameters data structure
     */
    private Params parseCommand(String[] args) throws Exception {
        Options options = new Options();
        Option o_filename = new Option("f", "filename",  true, "file to distribute");
        o_filename.setRequired(true);
        options.addOption(o_filename);
        Option o_destination = new Option("d", "destination", true, "destination folder");
        o_destination.setRequired(true);
        options.addOption(o_destination);
        Option o_cluster = new Option("c", "cluster", true, "path to cluster config json file");
        o_cluster.setRequired(false);
        options.addOption(o_cluster);
        Option o_linenumber = new Option("l", "lines", true, "number of rows in each chunk");
        o_linenumber.setRequired(false);
        options.addOption(o_linenumber);
        Option o_format = new Option("o", "orc", false, "save file as orc");
        o_format.setRequired(false);
        options.addOption(o_format);
        Option o_schema = new Option("s", "schema", true, "input schema file");
        o_schema.setRequired(false);
        options.addOption(o_schema);
        Option o_pattern = new Option("p", "logpattern", true, "if supplied it's a Grok pattern used to parse the input log file");
        o_pattern.setRequired(false);
        options.addOption(o_pattern);
        Option o_header = new Option("h", "header", false, "set if file has header row");
        o_header.setRequired(false);
        options.addOption(o_header);
        Option o_fewercolumns = new Option("w", "fewer", false, "set if rows with fewer columns are allowed");
        o_fewercolumns.setRequired(false);
        options.addOption(o_fewercolumns);
        Option o_skip = new Option("w", "skip", true, "number of lines to skip before starting parsing");
        o_skip.setRequired(false);
        options.addOption(o_skip);
        /*
         * todo: support the -D directory option for a list of files.
        Option o_directory = new Option("D", "Directory", true,
                "path to directory with the files to send (not supported yet)");
        o_directory.setRequired(false);
        options.addOption(o_directory);
         */

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd == null)
                throw new ParseException("null command line");
        }
        catch (ParseException pe) {
            System.out.println("can't parse due to " + pe);
            usage(options);
            System.exit(1);
        }
        Params parameters = new Params();
        try{
            /*
            if (cmd.hasOption('f') == cmd.hasOption('D'))
                throw new RuntimeException("need either file or directory");
             */
            if (cmd.hasOption('f')) {
                parameters.filename = cmd.getOptionValue('f');
                // parameters.fileList.add(cmd.getOptionValue('f'));
            }
            /*
            else {
                parameters.directory = cmd.getOptionValue('D');
                File folder = new File(cmd.getOptionValue('D'));
                File[] lFiles = folder.listFiles();
                if (lFiles == null)
                    throw new RuntimeException("No files found");
                for (File lFile : lFiles)
                    if (lFile.isFile())
                        parameters.fileList.add(parameters.directory + lFile.getName());
                }
             */
        } catch (RuntimeException e) {
            error(e);
        }
        parameters.destinationFolder = cmd.getOptionValue('d');
        parameters.cluster = cmd.getOptionValue("cluster");
        parameters.grokPattern = cmd.getOptionValue("logpattern");
        if (cmd.hasOption('l')) {
            try {
                parameters.chunkSize = Integer.parseInt(cmd.getOptionValue('l'));
            } catch (NumberFormatException e) {
                usage(options);
                System.out.println("Can't parse number due to " + e.getMessage());
            }
        }
        parameters.saveOrc = cmd.hasOption('o');
        if (cmd.hasOption('s'))
            parameters.inputSchemaName = cmd.getOptionValue('s');
        parameters.hasHeader = cmd.hasOption('h');
        parameters.allowFewerColumns = cmd.hasOption('w');
        if (cmd.hasOption("skip")) {
            try {
                parameters.skipLines = Integer.parseInt(cmd.getOptionValue("skip"));
            } catch (NumberFormatException e) {
                usage(options);
                System.out.println("Can't parse number due to " + e.getMessage());
            }
        }
        return parameters;
    }

    public static void main(String[] args) throws Exception {
        DataUpload upload = new DataUpload();
        upload.run(args);
    }

    public int run(String... args) throws Exception {
        Params parameters = parseCommand(args);
        int parts = 0;
        try {
            @Nullable
            ClusterConfig config = null;
            if (parameters.cluster != null)
                config = ClusterConfig.parse(parameters.cluster);
            CsvFileLoader.Config parsConfig = new CsvFileLoader.Config();
            parsConfig.hasHeaderRow = parameters.hasHeader;
            parsConfig.allowFewerColumns = parameters.allowFewerColumns;

            TextFileLoader loader;
            if (parameters.grokPattern != null) {
                GrokLogs logs = new GrokLogs(parameters.grokPattern);
                GrokLogs.LogFileLoader ldr = logs.getFileLoader(parameters.filename);
                ldr.addFixedColumns = false;  // we don't need these
                loader = ldr;
            } else {
                loader = new CsvFileLoader(parameters.filename, parsConfig, parameters.inputSchemaName);
            }
            parts = this.chop(loader, config, parameters);

            String localSchemaFile;
            String outputSchemaFile;
            if (!Utilities.isNullOrEmpty(parameters.inputSchemaName)) {
                localSchemaFile = parameters.inputSchemaName;
                outputSchemaFile = FilenameUtils.getBaseName(parameters.inputSchemaName);
            } else {
                if (this.tableSchema == null)
                    throw new RuntimeException("Could not guess table schema");
                int i = 1;
                localSchemaFile = parameters.defaultSchemaName;
                while (Files.exists(Paths.get(localSchemaFile))) {
                    localSchemaFile = parameters.defaultSchemaName + i;
                    i++;
                }
                outputSchemaFile = parameters.defaultSchemaName;
                HillviewLogger.instance.info("Writing schema", "{0}", localSchemaFile);
                this.tableSchema.writeToJsonFile(Paths.get(localSchemaFile));
            }

            // Create directories and place the schema
            if (config != null && config.workers != null) {
                for (String host : config.workers) {
                    createDir(Converters.checkNull(config.user), host, parameters.destinationFolder);
                    sendFile(localSchemaFile, config.user, host, parameters.destinationFolder, outputSchemaFile);
                }
                if (Utilities.isNullOrEmpty(parameters.inputSchemaName))
                    // We have created this schema file
                    Files.delete(Paths.get(localSchemaFile));
            } else {
                if (parameters.inputSchemaName == null)
                    Files.move(Paths.get(localSchemaFile),
                            Paths.get(parameters.destinationFolder, outputSchemaFile));
            }

            System.out.println("Done; created " + parts + " files");
        } catch (Exception e) {
            this.error(e);
        }
        return parts;
    }

    @Nullable
    private Schema tableSchema;

    private int chop(TextFileLoader loader,
                     @Nullable ClusterConfig clusterConfig,
                     Params parameters) throws Exception {
        loader.prepareLoading();
        int currentHost = 0;
        String chunkName;
        int chunk = 0;

        if (parameters.skipLines > 0)
            loader.loadFragment(parameters.skipLines, true);
        while (true) {
            ITable table = loader.loadFragment(parameters.chunkSize, false);
            if (chunk > 0 && table.getNumOfRows() == 0)
                // If the first chunk is empty generate it anyway.
                break;
            tableSchema = table.getSchema();
            while (true) {
                chunkName = getFileName(parameters.filename).concat(Integer.toString(chunk));
                if (parameters.saveOrc)
                    chunkName = chunkName.concat(".orc");
                else
                    chunkName = chunkName.concat(".csv");
                if (Files.exists(Paths.get(chunkName)))
                    chunk++;
                else
                    break;
            }
            writeTable(table, chunkName, parameters.saveOrc);
            if (clusterConfig != null) {
                assert clusterConfig.workers != null;
                String host = clusterConfig.workers[currentHost];
                sendFile(chunkName, Converters.checkNull(clusterConfig.user),
                        host, parameters.destinationFolder, chunkName);
                currentHost = (currentHost + 1) % clusterConfig.workers.length;
                Files.deleteIfExists(Paths.get(chunkName));
            } else {
                Files.move(Paths.get(chunkName), Paths.get(parameters.destinationFolder, chunkName));
            }
            chunk++;
            if (table.getNumOfRows() == 0)
                break;
        }
        loader.endLoading();
        return chunk;
    }

    private static String getFileName(String fileName) {
        Path p = Paths.get(fileName);
        String name = p.getFileName().toString();
        int loc = name.lastIndexOf(".");
        if (loc >= 0)
            name = name.substring(0,loc);
        return name;
    }

    private void createDir(String user, String host, String remoteFolder) throws Exception {
        HillviewLogger.instance.info("Creating folder " + remoteFolder + " at " + host);
        String[] commands = new String[]{"ssh", user + "@" + host, "mkdir", "-p", remoteFolder};
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        int err = process.waitFor();
        if (err != 0)
            throw new RuntimeException("mkdir stopped with error code " + err);
    }

    /**
     * Send file to host using SSH
     * @param filename file to send
     * @param host host to send to
     */
    private void sendFile(String filename, String user, String host, String remoteFolder, String remoteFile)
            throws Exception {
        HillviewLogger.instance.info("attempting to send file " + filename + " to " + host);
        String[] commands = new String[]{"scp", filename, user + "@" + host + ":" + remoteFolder + "/" + remoteFile};
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);

        Process process = pb.start();
        int err = process.waitFor();
        if (err != 0)
            throw new RuntimeException("scp stopped with error code " + err);
    }

    /** Writes the table in ORC or CSV format
     */
    private void writeTable(ITable table, String filename, boolean orc) {
        HillviewLogger.instance.info("Writing chunk in: " + filename);
        if (orc) {
            OrcFileWriter writer = new OrcFileWriter(filename);
            writer.writeTable(table);
        } else {
            CsvFileWriter writer = new CsvFileWriter(filename);
            writer.writeTable(table);
        }
    }

    private void error(Exception ex) throws Exception {
        // Unfortunately ex.getMessage() is often useless.
        ex.printStackTrace();
        throw ex;
    }
}
