package org.hillview.test;

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
import org.junit.Test;
import org.apache.commons.cli.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class DataUpload  {
    private IAppendableColumn[] columns;
    private int currentField = 0;
    private GuessSchema[] schemaGuesses;
    private final int defaultChunkSize = 1000000;
    private final String usageString = "Usage: DataUpload " +
            "-f <filename> " +
            "-d <remoteFolder> " +
            "-c <clusterConfig> " +
            "-l <linenumber> " +
            "-o (if save as orc) " +
            "-s <schema> " +
            "-h (if has header row)" +
            "-w (if allow fewer columns)";
    private final String defaultSchemaName = "schema.schema";
    @Nullable
    private String schemaPath = null;
    private String filename = ""; // the file to be sent
    private String remoteFolder = ""; // the destination path where the files will be put
    private String cluster = ""; // the path to the cluster config json file
    private boolean hasHeader;
    private boolean orc; // true if saving as orc, otherwise save as csv;
    private int chunkSize; // the number of lines in each shard.
    private boolean allowFewerColumns = false;

    private void usage(String usage, Options options) {
        System.out.println(usage);
        System.out.println(options.getOptions());
    }

    private String[] createArgs() {
        String[] args = new String[]{ "-f", "/users/uwieder/Projects/Data/Pitching.csv",
                "--cluster", "/Users/uwieder/Projects/Hillview/Hillview/bin/config.json",
                "-d", "/dataUpload/",
                "-l", "1000",
                "-h"};
        return args;
    }

    private void parseCommand(String[] args) {
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
        Option o_format = new Option("o","orc",false, "when appears the file will be saved as orc");
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
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            this.filename = cmd.getOptionValue('f');
            this.remoteFolder = cmd.getOptionValue('d');
            this.cluster = cmd.getOptionValue("cluster");
            if (cmd.hasOption('l')) {
                try {
                    this.chunkSize = Integer.parseInt(cmd.getOptionValue('l'));
                } catch (NumberFormatException e) {
                    this.error(e.getMessage());
                    this.usage(usageString, options);
                }
            }
            else
                this.chunkSize = this.defaultChunkSize;
            this.orc = cmd.hasOption('o');
            if (cmd.hasOption('s'))
                this.schemaPath = cmd.getOptionValue('s');
            this.hasHeader = cmd.hasOption('h');
            this.allowFewerColumns = cmd.hasOption('w');
        }
        catch (ParseException pe) {
            System.out.println("can't parse due to " + pe);
            usage(this.usageString, options);
        }
    }

    @Test
    public void uploadFile() {
        String[] args = this.createArgs();
        parseCommand(args);
        try {
            ClusterConfig config = ClusterConfig.parse(this.cluster);
            CsvFileLoader.Config parsConfig = new CsvFileLoader.Config();
            parsConfig.hasHeaderRow = this.hasHeader;
            parsConfig.allowFewerColumns = this.allowFewerColumns;
            Schema mySchema;
            if (!Utilities.isNullOrEmpty(this.schemaPath))
                mySchema = Schema.readFromJsonFile(Paths.get(this.schemaPath));
            else {
                mySchema = this.guessSchema(filename, parsConfig);
                this.schemaPath = this.defaultSchemaName;
                mySchema.writeToJsonFile(Paths.get(this.schemaPath));
            }
            chop_files(parsConfig, config, mySchema);
        } catch(IOException e) {
            System.out.println(e);
        }
    }

    private Schema guessSchema(String filename, CsvFileLoader.Config config) {
        Reader file = null;
        try {
            file = this.getFileReader(filename);
            CsvParserSettings settings = new CsvParserSettings();
            CsvFormat format = new CsvFormat();
            format.setDelimiter(config.separator);
            settings.setFormat(format);
            settings.setIgnoreTrailingWhitespaces(true);
            settings.setEmptyValue("");
            settings.setNullValue(null);
            settings.setReadInputOnSeparateThread(false);
            settings.setMaxColumns(50000);
            CsvParser myParser = new CsvParser(settings);
            myParser.beginParsing(file);
            @javax.annotation.Nullable
            String[] line = null;
            try {
                line = myParser.parseNext();
            } catch (Exception ex) {
                System.out.println(ex);
            }
            if (line == null)
                throw new RuntimeException("Missing header row " + filename);
            this.schemaGuesses = new GuessSchema[line.length];
            if (this.hasHeader)
                HillviewLogger.instance.info("Creating schema from Header row");
            else
                HillviewLogger.instance.info("Creating schema from first row");
            int index = 0;
            for (String col : line) {
                this.schemaGuesses[index] = new GuessSchema();
                if (this.hasHeader)
                    this.schemaGuesses[index].setName(col);
                else {
                    this.schemaGuesses[index].setName("Column_" + Integer.toString(index));
                    this.schemaGuesses[index].updateGuess(col);
                }
                index++;
            }
            while (true) {
                @Nullable
                String[] nextLine = null;
                try {
                    nextLine = myParser.parseNext();
                } catch (RuntimeException ex) {
                    this.error(ex.getMessage());
                }
                if (nextLine == null)
                    break;
                index = 0;
                for (String col : nextLine) {
                    this.schemaGuesses[index].updateGuess(col);
                    index++;
                }
            }
            myParser.stopParsing();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        Schema schema = new Schema();
        for (int i = 0; i < this.schemaGuesses.length; i++) {
            schema.append(this.schemaGuesses[i].getColumnDesc());
        }
        return schema;
    }

    /**
     *
     * @param config the configuration fo the parser to be used
     * @param schema the schema of the file. Null if there is no schema and needs to be guessed.
     */
    private void chop_files(CsvFileLoader.Config config, ClusterConfig clusterConfig, Schema schema) {

        Reader file = null;
        try {
            file = this.getFileReader(this.filename);
            CsvParserSettings settings = new CsvParserSettings();
            CsvFormat format = new CsvFormat();
            format.setDelimiter(config.separator);
            settings.setFormat(format);
            settings.setIgnoreTrailingWhitespaces(true);
            settings.setEmptyValue("");
            settings.setNullValue(null);
            settings.setReadInputOnSeparateThread(false);
            settings.setMaxColumns(schema.getColumnCount());
            CsvParser myParser = new CsvParser(settings);
            myParser.beginParsing(file);
            if (config.hasHeaderRow) {
                @javax.annotation.Nullable
                String[] line = null;
                try {
                    line = myParser.parseNext();
                } catch (Exception ex) {
                    System.out.println(ex);
                }
                if (line == null)
                    throw new RuntimeException("Missing header row " + this.filename);
            }
            this.columns = schema.createAppendableColumns();
            int chunk = 0;
            // Start Creating the chunks
            // *********************************************
            for (String host : clusterConfig.backends) {
                createDir(clusterConfig.user, host);
                sendFile(this.schemaPath, clusterConfig.user, host);
            }

            boolean more_chunks = true;
            int currentHost = 0;
            while(more_chunks) {
                for (int i = 0; i < this.chunkSize; i++) {
                    @javax.annotation.Nullable
                    String[] line = null;
                    try {
                        line = myParser.parseNext();
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                    if (line == null) {
                        more_chunks = false;
                        break;
                    }
                    this.append(line);
                }

                IColumn[] sealed = new IColumn[this.columns.length];
                IMembershipSet ms = null;
                for (int ci = 0; ci < this.columns.length; ci++) {
                    IAppendableColumn c = this.columns[ci];
                    IColumn s = c.seal();
                    if (ms == null)
                        ms = new FullMembershipSet(s.sizeInRows());
                    sealed[ci] = s;
                    assert sealed[ci] != null;
                }
                Table table  = new Table(sealed, null, null);
                String chunkName = "chunk".concat(Integer.toString(chunk));
                if (this.orc)
                    chunkName = chunkName.concat(".orc");
                else
                    chunkName = chunkName.concat(".csv");
                writeTable(table, chunkName, this.orc);
                String host = clusterConfig.backends[currentHost];
                sendFile(chunkName, clusterConfig.user, host);
                Files.deleteIfExists(Paths.get(chunkName));
                currentHost = (currentHost + 1) % clusterConfig.backends.length;
                chunk++;
                this.columns = schema.createAppendableColumns();
            }
            myParser.stopParsing();
        } catch(Exception e) {
            this.error(e.getMessage());
        }
    }

    private void createDir(String user, String host) throws Exception {
        HillviewLogger.instance.info("Creating folder " + this.remoteFolder + " at " + host);
        String[] commands = new String[]{"ssh", user + "@" + host, "mkdir", "-p", this.remoteFolder};
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
    private void sendFile(String filename, String user, String host) throws Exception {
        HillviewLogger.instance.info("attempting to send file " + filename + " to " + host);
        String[] commands = new String[]{"scp", filename, user + "@" + host + ":" + this.remoteFolder};
        ProcessBuilder pb = new ProcessBuilder(commands);
        pb.redirectErrorStream(true);

            Process process = pb.start();
            int err = process.waitFor();
            if (err != 0)
                throw new RuntimeException("Scp stopped with error code " + Integer.toString(err));
    }
    
    /** Writes the table in ORC or CSV formatt
     *
     * @param table
     * @param filename
     */
    private void writeTable(Table table, String filename, boolean orc) {
        HillviewLogger.instance.info("Writing chunk in: " + filename);
        if (orc) {
            OrcFileWriter writer = new OrcFileWriter(filename);
            writer.writeTable(table);
        } else {
            CsvFileWriter writer = new CsvFileWriter(filename);
            writer.writeTable(table);
        }
    }

    private void append(String[] data) {
        try {
            assert this.columns != null;
            int columnCount = this.columns.length;
            int currentColumn = 0;
            String currentToken;
            if (data.length > columnCount)
                this.error("Too many columns " + data.length + " vs " + columnCount);
            for (currentColumn = 0; currentColumn < data.length; currentColumn++) {
                currentToken = data[currentColumn];
                this.columns[currentColumn].parseAndAppendString(currentToken);
                this.currentField++;
                if ((this.currentField % 100000) == 0) {
                    System.out.print(".");
                    System.out.flush();
                }
            }
            if (data.length < columnCount) {
                if (!this.allowFewerColumns)
                    this.error("Too few columns " + data.length + " vs " + columnCount);
                else {
                    currentToken = "";
                    for (int i = data.length; i < columnCount; i++)
                        this.columns[i].parseAndAppendString(currentToken);
                }
            }
        } catch (Exception ex) {
            this.error(ex.getMessage());
        }
    }

    private Reader getFileReader(String filename) {
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
    private void error(String mess) {
        System.out.println(mess);
    }
}