package org.hillview.test;

import org.junit.Test;
import org.apache.commons.cli.*;

import java.io.*;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;


public class DataUpload {

    private String[] createArgs() {
        String[] args = new String[4];
        args[0] = "-d";
        args[1] = "testing";
        args[2] = "-L";
        args[3] = "link";
        return args;
    }

    private class myParcer {
        public String webserver;
        public int backend_port;
        public String user;
        public String[] backends = { "worker1.name", "worker2.name" }; //todo hard-wired for now


        public  myParcer(String fileName) {

            try {
                File file = new File(fileName);
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if (line.length() == 0) continue;
                    line = line.replaceAll("\\s+",""); // remove all white spaces
                    line = line.split("#")[0]; // remove comments
                    String[] tokens = line.split("=", 2);

                    if (tokens[0].equals("webserver")) {
                        this.webserver = tokens[1].substring(1, tokens[1].length() - 1);
                        System.out.println("Web Server is: " + webserver);
                    }

                    if (tokens[0].equals("backend_port")) {
                        this.backend_port = Integer.valueOf(tokens[1]);
                        System.out.println("backend port is: " + backend_port);
                    }

                    if (tokens[0].equals("user")) {
                        this.user = tokens[1].substring(1, tokens[1].length() - 1);
                        System.out.println("user is: " + user);
                    }

                    if (tokens[0].equals("backends")) {
                        System.out.println("hmmm what to do with backends?");
                    }

                }

                fileReader.close();
            }
            catch(IOException ie) {
                System.out.println(ie);
            }
        }
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

        /* todo: Add usage conditional */

        String theInput = cmd.getOptionValue('d');
        System.out.println("Got it, it's " + theInput);
        String theInput1 = cmd.getOptionValue('L');
        System.out.println("Got it, it's " + theInput1);
        String theInput2 = cmd.getOptionValue('s');
        System.out.println("Got it, it's " + theInput2);

        myParcer myparcer = new myParcer("/Users/uwieder/Projects/Bigdata/Hillview/Hillview/bin/config.py");

        String[] fileList = { "file1.csv", "file2.csv" }; //todo hard-wired for now

        copy_files(myparcer, theInput, fileList, theInput1);

        //todo: for now assume folder is an absolute path
    }

    private void copy_files(myParcer config, String folder, String[] fileList, String copyOption) {
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

    //extend textFileLoader
    private void chop_file(String fileName, int chunks) {
        Reader file = null;
        try {
            //file = this.getFileReader();
            CsvParserSettings settings = new CsvParserSettings();
            CsvFormat format = new CsvFormat();
        } catch (Exception e) {
            System.out.println("exception chopping the files: " + e);
        }
    }


}