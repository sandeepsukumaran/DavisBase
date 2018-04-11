/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.github.sandeepsukumaran.davisbase.main;

import io.github.sandeepsukumaran.davisbase.exception.InvalidQuerySyntaxException;
import java.util.Scanner;

import io.github.sandeepsukumaran.davisbase.query.queryParser;
import io.github.sandeepsukumaran.davisbase.exception.NoDatabaseSelectedException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
/**
 *
 * @author Sandeep
 */
public class DavisBase {

    /**
     * Display the welcome message.
     */
    private static void displaySplash(){
        System.out.println("Welcome to DavisBase. Commands end with ;");
        System.out.println("Server version: "+VERSIONSTRING+"\n");
        System.out.println(COPYRIGHTSTRING+"\n");
        System.out.println("Type \'help;\' for help.\n");
    }
    
    /**
     * Look for and set-up required file system at each program startup. 
     * @return -1 if file structure is not properly set up.
     */
    private static int setupFS(){
        String workingDirectory = System.getProperty("user.dir");
        Path dataFolderPath = Paths.get(workingDirectory+FileSystems.getDefault().getSeparator()+"data");
        if (Files.exists(dataFolderPath)){
            if(!Files.isReadable(dataFolderPath)){
                System.out.println("User does not have permission to read files in this location.");
                return -1;
            }else if(!Files.isWritable(dataFolderPath)){
                System.out.println("WARNING: User does not have write permission in this location. Read only mode.");
                return 0;
            }
        }else if(!Files.isWritable(Paths.get(workingDirectory))){
            System.out.println("ERROR : Unable to setup file structures. Terminating...");
            return -1;
        }else{
            //File structure doesn't exist but can be created.
            try{
                Files.createDirectories(Paths.get(dataFolderPath.toString()+FileSystems.getDefault().getSeparator()+"catalog"));
                Files.createFile(Paths.get(dataFolderPath.toString()+FileSystems.getDefault().getSeparator()+"catalog"+FileSystems.getDefault().getSeparator()+"davisbase_tables.tbl"));
                Files.createFile(Paths.get(dataFolderPath.toString()+FileSystems.getDefault().getSeparator()+"catalog"+FileSystems.getDefault().getSeparator()+"davisbase_columns.tbl"));
                Files.createDirectory(Paths.get(dataFolderPath.toString()+FileSystems.getDefault().getSeparator()+"user_data"));
            }catch(IOException e){
                System.out.println("ERROR : Unable to setup file structures. Terminating...");
                return -1;
            }
        }
        return 0;
    }
    
    /**
     * @param args the command line arguments (ignored)
     */
    public static void main(String[] args) {
        //display startup splash screen
        displaySplash();
        if (setupFS() == -1){
            return;
        }else;
        
        String inputString;
        
        while(!exitFlag){
            //display prompt
            System.out.print(promptText);
            //Read and sanitize user input
            inputString = inputStream.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
            //Parse input and perform operations
            try{
                queryParser.parseInputCommand(inputString);
            }catch(NoDatabaseSelectedException e){
                System.out.println("\nERROR 1046 : No database selected.");
            }catch(InvalidQuerySyntaxException|NoSuchTableException|NoSuchColumnException e){
                System.out.println(e);
            }
        }
        
        System.out.println("\nBye\n\n");
    }
    
    /**
     * Read table names from davisbase_tables.tbl file and populate list of table names.
     */
    public static void populateTableNames(){
        //read from davisbase_tables.tbl and get the list of table names
        tableNames = new ArrayList<>();
    }
    
    /**
     * Get the names of tables in current database.
     * @return names of tables as ArrayList of String.
     */
    public static ArrayList<String> getTableNames(){
        return tableNames;
    }
    
    /**
     * Returns names of columns in given table by reading davisbase_columns.tbl file
     * @param tableName Name of table
     * @return ArrayList of String containing names of columns in table in ordinal order.
     */
    public static ArrayList<String> getTableColumns(String tableName){
        ArrayList<String> colNames = null;
        return colNames;
    }

    //Variable declarations
    /**< Set to true to indicate program must terminate.*/public static boolean exitFlag = false;
    /**< Variable attached to STDIN to read user inputs, delimited by ;*/static Scanner inputStream = new Scanner(System.in).useDelimiter(";");
    /**< Name of currently selected database.*/public static String activeDatabase = "";//null - use this if implementing databases feature
    public static ArrayList<String> tableNames;
    
    /**< Variable holding the text string displayed as prompt. Terminated with >.*/static String promptText = "davisql>";
    /**< String describing version number.*/public static final String VERSIONSTRING = "0.1.0";
    /**< Copyright message.*/static final String COPYRIGHTSTRING = "Copyright (c) 2018, Sandeep Sukumaran. All rights reserved.";
    /**< Page size used by database application.*/ static final long PAGESIZE = 512;
}
