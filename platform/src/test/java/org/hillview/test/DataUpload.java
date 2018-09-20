package org.hillview.test;

import javax.annotation.Nullable;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.hillview.management.ClusterConfig;
import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.storage.OrcFileWriter;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.rows.GuessSchema;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import org.junit.Test;
import org.apache.commons.cli.*;
import java.nio.file.Path;
import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;


import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;


public class DataUpload  {

    IAppendableColumn[] columns;
    boolean allowFewerColumns;
    private int currentRow;
    private int currentField = 0;
    Schema schema = null;
    GuessSchema[] schemaGuesses;

    @Nullable String schemaPath;
    String directory;
    String filename;
    String destination;
    boolean hasHeader;
    int chunkSize;
    int columnNumber;

    private String[] createArgs() {
        String[] args = new String[4];
        args[0] = "-d";
        args[1] = "testing";
        args[2] = "-L";
        args[3] = "link";
        return args;
    }


    @Test
    public void uploadFile() {
        String[] args = createArgs();
        Options options = new Options();

        Option folder = new Option("d", "destination", true, "destination folder where output is written" +
                " (if relative it is with respect to config.service_folder)");
        folder.setRequired(true);
        options.addOption(folder);

        Option copyOption = new Option("L","L",true, "Follow symlinks instead of ignoring them)");
        copyOption.setRequired(true);
        options.addOption(copyOption);

        Option everywhere = new Option("s","s",true, "File that is loaded to all machines");
        everywhere.setRequired(false);
        options.addOption(everywhere);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        }
        catch (ParseException pe) {
            System.out.println("can't parse due to " + pe);
        }
        String theInput = cmd.getOptionValue('d');
        System.out.println("Got it, it's " + theInput);
        String theInput1 = cmd.getOptionValue('L');
        System.out.println("Got it, it's " + theInput1);
        String theInput2 = cmd.getOptionValue('s');
        System.out.println("Got it, it's " + theInput2);

        /* todo: Add usage conditional */





        this.hasHeader = true;
        this.directory = "/Users/uwieder/Projects/Bigdata/Hillview/data/";
        this.filename = "/Users/uwieder/Projects/Bigdata/Hillview/data/pitchingTest.csv";
        this.destination = "/Users/uwieder/Projects/Bigdata/Hillview/data/";
        this.schemaPath = null;

        try {
            ClusterConfig config = ClusterConfig.parse("/Users/uwieder/Projects/Bigdata/Hillview/Hillview/bin/config.json");
            // todo: create the CsvFileLoader.Config object. Should come from the command line.
            CsvFileLoader.Config parsConfig = new CsvFileLoader.Config();

            parsConfig.hasHeaderRow = this.hasHeader;
            Schema mySchema;
            if (!Utilities.isNullOrEmpty(this.schemaPath))
                mySchema = Schema.readFromJsonFile(Paths.get(this.schemaPath));
            else {
                mySchema = this.guessSchema(filename, parsConfig);
            }
            Path schemaPath  = Paths.get(this.destination + "bestGuess.schema");
            mySchema.writeToJsonFile(schemaPath);

            //todo: Send the schema file to each of the machines

            chop_files(this.filename, this.destination,100, parsConfig, config, mySchema,false);
            // todo: copy_files()

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

            this.columnNumber = line.length;
            this.schemaGuesses = new GuessSchema[this.columnNumber];

            if (config.hasHeaderRow)
                HillviewLogger.instance.info("Creating schema from Header row");
            else
                HillviewLogger.instance.info("Creating schema from first row");

            int index = 0;
            for (String col : line) {
                this.schemaGuesses[index] = new GuessSchema();
                if (config.hasHeaderRow)
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
                } catch (Exception ex) {
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
        for (int i = 0; i < columnNumber; i++) {
            schema.append(this.schemaGuesses[i].getColumnDesc());
        }
        return schema;
    }

    /**
     *
     * @param filename file to chop
     * @param destination location into which the chopped files are placed
     * @param lines number of lines in each chopped file
     * @param config the configuration fo the parser to be used
     * @param schema the schema of the file. Null if there is no schema and needs to be guessed.
     */

    private void chop_files(String filename, String destination, int lines, CsvFileLoader.Config config, ClusterConfig clusterConfig, Schema schema, boolean orc) {

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

            this.allowFewerColumns = config.allowFewerColumns;
            settings.setMaxColumns(schema.getColumnCount());
            CsvParser myParser = new CsvParser(settings);
            myParser.beginParsing(file);

            this.currentRow = 0;

            if (config.hasHeaderRow) {
                @javax.annotation.Nullable
                String[] line = null;
                try {
                    line = myParser.parseNext();
                } catch (Exception ex) {
                    System.out.println(ex);
                }
                if (line == null)
                    throw new RuntimeException("Missing header row " + filename);
                this.currentRow++;
            }
            this.columns = schema.createAppendableColumns();

            int chunk = 0;

            // Start Creating the Buffers
            // *********************************************
            boolean more_chunks = true;
            int currentHost = 0;
            boolean[] sentSchema = new boolean [clusterConfig.backends.length];
            Arrays.fill(sentSchema, Boolean.FALSE);
            while(more_chunks) {

                for (int i = 0; i < lines; i++) {
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
                Table table  = new Table(sealed, filename, null);
                String chunkName = destination.concat(Integer.toString(chunk));
                if (orc)
                    chunkName = chunkName.concat(".orc");
                else
                    chunkName = chunkName.concat(".csv");
                writeTable(table, chunkName, orc);
                String host = clusterConfig.backends[currentHost];
                String remoteFolder = clusterConfig.service_folder;
                if (!sentSchema[currentHost])
                    sendFile(this.destination + " guessSchema.schema", remoteFolder, clusterConfig.user, host, false);
                sendFile(chunkName, remoteFolder, clusterConfig.user, host,true);
                currentHost = (currentHost + 1) % clusterConfig.backends.length;
                chunk++;
                this.columns = schema.createAppendableColumns();
            }
            myParser.stopParsing();

        } catch(Exception e) {
            this.error(e.getMessage());
        }
    }

    /**
     * Send file to host using SSH
     * @param filename file to send
     * @param host host to send to
     * @param deleteFile should the file be deleted after it is sent?
     */
    private void sendFile(String filename, String remoteFolder, String user, String host, boolean deleteFile ) {
        JSch jsch = new JSch();
        try {
            Session session = jsch.getSession(user, host);
            session.connect();
            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();

            sftpChannel.put(filename, remoteFolder);
        } catch(Exception e) {
            this.error(e.getMessage());
        }


    }
    /** Writes the table in ORC or CSV formatt
     *
     * @param table
     * @param filename
     */
    private void writeTable(Table table, String filename, boolean orc) {
        System.out.println("Writing table in " + filename);
        if (orc) {
            OrcFileWriter writer = new OrcFileWriter(filename);
            writer.writeTable(table);
        } else {
            CsvFileWriter writer = new CsvFileWriter(filename);
            writer.writeTable(table);
        }
        // send file
        // delete file
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
            this.currentRow++;
        } catch (Exception ex) {
            this.error(ex.getMessage());
        }

    }

    private void copy_files(ClusterConfig config, String folder, String[] fileList, String copyOption) {
        System.out.println("Copying " + fileList.length + " files to all hosts");
        int index = 0;
        String f;
        String host;
        for (int i = 0; i < fileList.length; i++) {
            f = fileList[i];
            host = config.backends[index];
            index = (index + 1) % ((config.backends).length);
        //    rh = RemoteHost(config.user, host)
        //    copy_file_to_remote_host(rh, f, folder, copyOption)
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