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
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.hillview.management.ClusterConfig;
import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.storage.OrcFileWriter;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.rows.GuessSchema;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import org.apache.commons.cli.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * This entry point is only used for uploading data to a cluster. The user provides configuration details such as the
 * path to the cluster config file, the location of the file to upload etc. If a schema is not provided the program
 * scans the file to determine one. It then proceeds to read the file as a stream, and  chop it to smaller shards,
 * placing one shard in each server in a round robin fashion. The user chooses whether to upload the file in csv
 * format or in orc format. A schema file is also placed in each server.
 *
 */
public class DataUpload {
    private static class Params {
        final int defaultChunkSize = 1000000;
        final String defaultSchemaName = "schema";
        @Nullable
        String schemaPath = null;
        @Nullable
        String filename; // the file to be sent
        @Nullable
        String directory;
        final ArrayList<String> fileList = new ArrayList<String>();
        String remoteFolder = ""; // the destination path where the files will be put
        String cluster = ""; // the path to the cluster config json file
        boolean hasHeader; // true if file has a header row
        boolean orc; // true if saving as orc, otherwise save as csv;
        int chunkSize = defaultChunkSize; // the number of lines in each shard.
        boolean allowFewerColumns;
    }


    private static void usage(Options options) {
        final String usageString = "Usage: DataUpload ";
        System.out.print(usageString);
        System.out.println(options.getOptions());
    }

    /**
     * Parses the command line and fills up the parameters data structure
     * todo: support the -D directory option for a list of files. Currently it is parsed but not used.
     */

    private static Params parseCommand(String[] args) {
        Options options = new Options();
        Option o_filename = new Option("f", "filename", true, "path to file to distribute");
        o_filename.setRequired(true);
        options.addOption(o_filename);
        Option o_destination = new Option("d","destination",true, "relative path to remote folder");
        o_destination.setRequired(true);
        options.addOption(o_destination);
        Option o_cluster = new Option("c","cluster",true, "path to cluster config json file");
        o_cluster.setRequired(true);
        options.addOption(o_cluster);
        Option o_linenumber = new Option("l","lines",true, "number of lines in each chunk");
        o_linenumber.setRequired(false);
        options.addOption(o_linenumber);
        Option o_format = new Option("o","orc",false, "indicates to save file as orc");
        o_format.setRequired(false);
        options.addOption(o_format);
        Option o_schema = new Option("s","schema",true, "path to the schema file");
        o_schema.setRequired(false);
        options.addOption(o_schema);
        Option o_header = new Option("h","header",false, "indicates file has header row");
        o_header.setRequired(false);
        options.addOption(o_header);
        Option o_fewercolumns = new Option("w","fewer",false, "indicates if fewer columns are allowed");
        o_fewercolumns.setRequired(false);
        options.addOption(o_fewercolumns);
        Option o_directory = new Option("D", "Directory", true,
                "path to directory with the files to send (not supported yet)");
        o_directory.setRequired(false);
        options.addOption(o_directory);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd == null)
                throw new ParseException("null command line");
        }
        catch (ParseException pe) {
            System.out.println("can't parse due to " + pe);
            usage(options);
        }
        Params parameters = new Params();
        try{
            if (cmd.hasOption('f') == cmd.hasOption('D'))
                throw new RuntimeException("need either file or directory");
            if (cmd.hasOption('f')) {
                parameters.filename = cmd.getOptionValue('f');
                parameters.fileList.add(cmd.getOptionValue('f'));
            }
            else {
                parameters.directory = cmd.getOptionValue('D');
                File folder = new File(cmd.getOptionValue('D'));
                File[] lFiles = folder.listFiles();
                for (int i = 0; i < lFiles.length; i++)
                    if (lFiles[i].isFile())
                        parameters.fileList.add(parameters.directory + lFiles[i].getName());
                }
        } catch (RuntimeException e) {
            error(e);
        }
        parameters.remoteFolder = cmd.getOptionValue('d');
        parameters.cluster = cmd.getOptionValue("cluster");
        if (cmd.hasOption('l')) {
            try {
                parameters.chunkSize = Integer.parseInt(cmd.getOptionValue('l'));
            } catch (NumberFormatException e) {
                usage(options);
                System.out.println("Can't parse number due to " + e.getMessage());
            }
        }
        parameters.orc = cmd.hasOption('o');
        if (cmd.hasOption('s'))
            parameters.schemaPath = cmd.getOptionValue('s');
        parameters.hasHeader = cmd.hasOption('h');
        parameters.allowFewerColumns = cmd.hasOption('w');
        return parameters;
    }

    public static void main(String args[]) {
        Params parameters = parseCommand(args);
        try {
            ClusterConfig config = ClusterConfig.parse(parameters.cluster);
            CsvFileLoader.Config parsConfig = new CsvFileLoader.Config();
            parsConfig.hasHeaderRow = parameters.hasHeader;
            parsConfig.allowFewerColumns = parameters.allowFewerColumns;
            Schema mySchema;
            if (!Utilities.isNullOrEmpty(parameters.schemaPath))
                mySchema = Schema.readFromJsonFile(Paths.get(parameters.schemaPath));
            else {
                int i = 1;
                String tmpSchemaName = parameters.defaultSchemaName;
                while (Files.exists(Paths.get(tmpSchemaName))) {
                    tmpSchemaName = parameters.defaultSchemaName + Integer.toString(i);
                    i++;
                }
                parameters.schemaPath = tmpSchemaName;
                if (i > 1)
                    HillviewLogger.instance.warn("default Schema name was taken. used: " + parameters.schemaPath);
                mySchema = guessSchema(parameters.filename, parsConfig);
                mySchema.writeToJsonFile(Paths.get(parameters.schemaPath));
            }
            chopFiles(parsConfig, config, mySchema, parameters);
        } catch(IOException e) {
            error(e);
        }
    }

    private static CsvParser getParser(CsvFileLoader.Config config, int columnCount) {
        CsvParserSettings settings = new CsvParserSettings();
        CsvFormat format = new CsvFormat();
        format.setDelimiter(config.separator);
        settings.setFormat(format);
        settings.setIgnoreTrailingWhitespaces(true);
        settings.setEmptyValue("");
        settings.setNullValue(null);
        settings.setReadInputOnSeparateThread(false);
        settings.setMaxColumns(columnCount);
        return(new CsvParser(settings));
    }

    /**
     * Guesses the schema of a table written as a csv file by streaming through the file.
     * @param filename name of the file containing the table
     * @param config configuration file for the parser
     */
    private static Schema guessSchema(String filename, CsvFileLoader.Config config) {
        Reader file = null;
        GuessSchema[] schemaGuesses = null;
        int column = 0;
        try {
            file = getFileReader(filename);
            CsvParser myParser = getParser(config, 50000);
            myParser.beginParsing(file);
            @Nullable
            String[] line = null;
            line = myParser.parseNext();
            if (line == null)
                throw new RuntimeException("Missing header row " + filename);
            column = line.length;
            schemaGuesses = new GuessSchema[column];
            int index = 0;
            if (config.hasHeaderRow)
                HillviewLogger.instance.info("Guessing column names using Header row");
            else
                HillviewLogger.instance.info("Guessing column names without Header row");

            for (String col : line) {
                schemaGuesses[index] = new GuessSchema();
                if (config.hasHeaderRow) {
                    schemaGuesses[index].setName(col);
                }
                else {
                    schemaGuesses[index].setName("Column_" + Integer.toString(index));
                    schemaGuesses[index].updateGuess(col);
                }
                index++;
            }
            HillviewLogger.instance.info("Guessing the schema");
            int progress = 1;
            while (true) {
                @Nullable
                String[] nextLine = null;
                nextLine = myParser.parseNext();
                if (nextLine == null)
                    break;
                index = 0;
                for (String col : nextLine) {
                    schemaGuesses[index].updateGuess(col);
                    index++;
                }
                if ((progress % 100000) == 0 )
                    System.out.print(".");
                progress++;
            }
            myParser.stopParsing();
        }
        catch (Exception e) {
            error(e);
        }
        Schema schema = new Schema();
        for (int i = 0; i < column; i++) {
            schema.append(schemaGuesses[i].getColumnDesc());
        }
        return schema;
    }

    /**
     * creates a directory in each remote host and places the schema there. Then it chops the file into shards and
     * sends the shards to remote hosts in round robin fashion. This is done with one streaming pass of the file.
     * @param config the configuration of the parser to be used
     * @param schema the schema of the file.
     * @param parameters the parameters taken from the command line, include file path and destination
     */
    private static void chopFiles(CsvFileLoader.Config config, ClusterConfig clusterConfig,
                                  Schema schema, Params parameters) {
        Reader file = null;
        IAppendableColumn[] columns = null;
        int progress = 0;
        String chunkName = "";
        try {
            file = getFileReader(parameters.filename);
            CsvParser myParser = getParser(config, schema.getColumnCount());
            myParser.beginParsing(file);
            if (config.hasHeaderRow) {
                @javax.annotation.Nullable
                String[] line = null;
                line = myParser.parseNext();
                if (line == null)
                    throw new RuntimeException("Missing header row " + parameters.filename);
            }
            columns = schema.createAppendableColumns();
            int chunk = 0;
            // Create directories and place the schema
            for (String host : clusterConfig.workers) {
                createDir(clusterConfig.user, host, parameters.remoteFolder);
                sendFile(parameters.schemaPath, clusterConfig.user, host, parameters.remoteFolder);
            }
            // Create and send the shards
            boolean moreChunks = true;
            int currentHost = 0;
            while(moreChunks) {
                for (int i = 0; i < parameters.chunkSize; i++) {
                    @javax.annotation.Nullable
                    String[] line = null;
                    try {
                        line = myParser.parseNext();
                    } catch (Exception ex) {
                        error(ex);
                    }
                    if (line == null) {
                        moreChunks = false;
                        break;
                    }
                    append(line, columns, config.allowFewerColumns);
                    progress = progress + 1;
                    if (progress % 10000 == 0)
                        System.out.print(".");
                }

                IColumn[] sealed = new IColumn[columns.length];
                IMembershipSet ms = null;
                for (int ci = 0; ci < columns.length; ci++) {
                    IAppendableColumn c = columns[ci];
                    IColumn s = c.seal();
                    if (ms == null)
                        ms = new FullMembershipSet(s.sizeInRows());
                    sealed[ci] = s;
                    assert sealed[ci] != null;
                }
                Table table = new Table(sealed, null, null);
                while (true) {
                    chunkName = getFileName(parameters.filename).concat(Integer.toString(chunk));
                if (parameters.orc)
                    chunkName = chunkName.concat(".orc");
                else
                    chunkName = chunkName.concat(".csv");
                if (Files.exists(Paths.get(chunkName)))
                    chunk++;
                else
                    break;
                }
                writeTable(table, chunkName, parameters.orc);
                String host = clusterConfig.workers[currentHost];
                sendFile(chunkName, clusterConfig.user, host, parameters.remoteFolder);
                Files.deleteIfExists(Paths.get(chunkName));
                currentHost = (currentHost + 1) % clusterConfig.workers.length;
                chunk++;
                columns = schema.createAppendableColumns();
            }
            myParser.stopParsing();
        } catch(Exception e) {
            try {
                Files.deleteIfExists(Paths.get(chunkName));
            } catch (Exception ex) {
                error(ex);
            }
            error(e);
        }
    }

    private static String getFileName(String fileName) {
        Path p = Paths.get(fileName);
        String name = p.getFileName().toString();
        int loc = name.lastIndexOf(".");
        if (loc >= 0)
            name = name.substring(0,loc);
        return name;
    }

    private static void createDir(String user, String host, String remoteFolder) throws Exception {
        HillviewLogger.instance.info("Creating folder " + remoteFolder + " at " + host);
        String[] commands = new String[]{"ssh", user + "@" + host, "mkdir", "-p", remoteFolder};
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        int err = process.waitFor();
        if (err != 0)
            throw new RuntimeException("mkdir stopped with error code " + Integer.toString(err));
    }

    /**
     * Send file to host using SSH
     * @param filename file to send
     * @param host host to send to
     */
    private static void sendFile(String filename, String user, String host, String remoteFolder) throws Exception {
        HillviewLogger.instance.info("attempting to send file " + filename + " to " + host);
        String[] commands = new String[]{"scp", filename, user + "@" + host + ":" + remoteFolder};
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);

            Process process = pb.start();
            int err = process.waitFor();
            if (err != 0)
                throw new RuntimeException("Scp stopped with error code " + Integer.toString(err));
    }

    /** Writes the table in ORC or CSV format
      */
    private static void writeTable(Table table, String filename, boolean orc) {
        HillviewLogger.instance.info("Writing chunk in: " + filename);
        if (orc) {
            OrcFileWriter writer = new OrcFileWriter(filename);
            writer.writeTable(table);
        } else {
            CsvFileWriter writer = new CsvFileWriter(filename);
            writer.writeTable(table);
        }
    }

    private static void append(String[] data, IAppendableColumn[] columns, boolean allowFewerColumns) {
        try {
            int columnCount = columns.length;
            int currentColumn = 0;
            String currentToken;
            if (data.length > columnCount)
                throw new RuntimeException("Too many columns " + data.length + " vs " + columnCount);
            for (currentColumn = 0; currentColumn < data.length; currentColumn++) {
                currentToken = data[currentColumn];
                columns[currentColumn].parseAndAppendString(currentToken);
            }
            if (data.length < columnCount) {
                if (!allowFewerColumns)
                    throw new RuntimeException("Too few columns " + data.length + " vs " + columnCount);
                else {
                    currentToken = "";
                    for (int i = data.length; i < columnCount; i++)
                        columns[i].parseAndAppendString(currentToken);
                }
            }
        } catch (RuntimeException ex) {
            error(ex);
        }
    }

    private static Reader getFileReader(String filename) {
        try {
            HillviewLogger.instance.info("Reading file", "{0}", filename);
            FileInputStream inputStream = new FileInputStream(filename);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            // The buffered input stream is needed by the CompressorStream
            // to detect the compression method at runtime.
            InputStream fis = bufferedInputStream;
            if (Utilities.isCompressed(filename)) {
                InputStream compressedStream = new CompressorStreamFactory()
                        .createCompressorInputStream(fis);
                fis = compressedStream;
            }
            BOMInputStream bomStream = new BOMInputStream(fis,
                    ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE,
                    ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE);
            ByteOrderMark bom = bomStream.getBOM();
            String charsetName = bom == null ? "UTF-8" : bom.getCharsetName();
            return new InputStreamReader(bomStream, charsetName);
        } catch (IOException|CompressorException e) {
            throw new RuntimeException(e);
        }
    }

    private static void error(Exception ex) {
        System.out.println(ex.getMessage());
    }
}
