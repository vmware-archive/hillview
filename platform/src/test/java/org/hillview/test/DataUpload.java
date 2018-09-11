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

import java.io.*;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;


public class DataUpload {

    IAppendableColumn[] columns;
    boolean allowFewerColumns;
    int currentRow;
    int currentField = 0;
    //String schemaPath;

    private String[] createArgs() {
        String[] args = new String[4];
        args[0] = "-d";
        args[1] = "testing";
        args[2] = "-L";
        args[3] = "link";
        return args;
    }





//    @Test
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


        try {
            ClusterConfig config = ClusterConfig.parse("/Users/uwieder/Projects/Bigdata/Hillview/Hillview/bin/config.json");
            System.out.println(config.webserver);
            // todo: create the CsvFileLoader.Config object
            // todo: read the schema file if it exists
            // todo: chop_files()
            CsvFileLoader.Config parsConfig = new CsvFileLoader.Config();
            chop_files("file.csv", "\\Users\\datafile_chop", 100000, parsConfig, null, false);
            // todo: copy_files()

        } catch(IOException e) {
            System.out.println(e);
        }

    }

    /**
     *
     * @param filename file to chop
     * @param destination location into which the chopped files are placed
     * @param lines number of lines in each chopped file
     * @param config the configuration fo the parser to be used
     * @param schema the schema of the file. Null if there is no schema and needs to be guessed.
     */

    private void chop_files(String filename, String destination, int lines, CsvFileLoader.Config config, @Nullable Schema schema, boolean orc) {

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
            Schema actualSchema = null;
            boolean guessSchema = true;

            this.allowFewerColumns = config.allowFewerColumns;
            if (schema != null) {
                settings.setMaxColumns(schema.getColumnCount());
                guessSchema = false;
            }
            else {
                settings.setMaxColumns(50000);
            }
            CsvParser myParser = new CsvParser(settings);
            myParser.beginParsing(file);
            this.currentRow = 0;
            String[] firstLine = null;
            if (guessSchema) {
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

                    HillviewLogger.instance.info("Creating schema");
                    actualSchema = new Schema();
                    int index = 0;
                    for (String col : line) {
                        if ((col == null) || col.isEmpty())
                            col = schema.newColumnName("Column_" + Integer.toString(index));
                        col = schema.newColumnName(col);
                        ColumnDescription cd = new ColumnDescription(col, ContentsKind.String);
                        actualSchema.append(cd);
                        index++;
                    }
                        this.currentRow++;
                } else { // create schema from first line

                    int columnCount;
                    actualSchema = new Schema();
                    firstLine = myParser.parseNext();
                    if (firstLine == null)
                        throw new RuntimeException("Cannot create schema from empty CSV file");
                    columnCount = firstLine.length;

                    for (int i = 0; i < columnCount; i++) {
                        ColumnDescription cd = new ColumnDescription("Column " + Integer.toString(i),
                                ContentsKind.String);
                        actualSchema.append(cd);
                        }
                }
                schema = actualSchema;
            }

            assert schema != null;

            /* todo: Write a Schema file */

            this.columns = schema.createAppendableColumns();

            if (firstLine != null)
                this.append(firstLine);

            int chunknum = 0;
            // Start Creating the Buffers
            // *********************************************
            boolean more_chunks = true;
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
                //myParser.stopParsing();
                IMembershipSet ms = null;
                for (int ci = 0; ci < this.columns.length; ci++) {
                    IAppendableColumn c = this.columns[ci];
                    IColumn s = c.seal();
                    if (ms == null)
                        ms = new FullMembershipSet(s.sizeInRows());
                    if (guessSchema) {
                        GuessSchema gs = new GuessSchema();
                        GuessSchema.SchemaInfo info = gs.guess((IStringColumn) s);
                        if (info.kind != ContentsKind.String &&
                                info.kind != ContentsKind.None)  // all elements are null
                            sealed[ci] = s.convertKind(info.kind, c.getName(), ms);
                        else
                            sealed[ci] = s;
                    } else {
                        sealed[ci] = s;
                    }
                    assert sealed[ci] != null;
                }

                writeTable(new Table(sealed, filename, null), destination.concat(Integer.toString(chunknum)), orc);
                chunknum++;
            }

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