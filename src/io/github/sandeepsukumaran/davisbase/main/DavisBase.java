/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.github.sandeepsukumaran.davisbase.main;

import io.github.sandeepsukumaran.davisbase.datatype.DataType;
import io.github.sandeepsukumaran.davisbase.exception.ArgumentCountMismatchException;
import io.github.sandeepsukumaran.davisbase.exception.BadInputValueException;
import io.github.sandeepsukumaran.davisbase.exception.ColumnCannotBeNullException;
import io.github.sandeepsukumaran.davisbase.exception.FileAccessException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidDataType;
import io.github.sandeepsukumaran.davisbase.exception.InvalidQuerySyntaxException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import java.util.Scanner;

import io.github.sandeepsukumaran.davisbase.query.queryParser;
import io.github.sandeepsukumaran.davisbase.exception.NoDatabaseSelectedException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import io.github.sandeepsukumaran.davisbase.helpermethods.HelperMethods;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
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
                return -1;
            }else{
                try {
                    populateTableNames();
                } catch (InvalidTableInformationException | IOException | MissingTableFileException e) {
                    System.out.println("ERROR : Unable to read information about file structures. Terminating...");
                    return -1;
                }
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
                HelperMethods.writeInitialMetaDataFiles();
                populateTableNames();
            }catch(InvalidTableInformationException | MissingTableFileException | IOException e){
                System.out.println("ERROR : Unable to setup file structures. Terminating...");
                System.out.println(e);
                e.printStackTrace();
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
            }catch(InvalidQuerySyntaxException|NoSuchTableException|NoSuchColumnException|FileAccessException|MissingTableFileException|InvalidDataType|ArgumentCountMismatchException | BadInputValueException | InvalidTableInformationException | IOException | ColumnCannotBeNullException e){
                System.out.println(e);
                e.printStackTrace();
            }
        }
        
        System.out.println("\nBye\n\n");
    }
    
    /**
     * Read table names from davisbase_tables.tbl file and populate list of table names.
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     * @throws java.io.IOException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     */
    public static void populateTableNames() throws InvalidTableInformationException, IOException, MissingTableFileException{
        //read from davisbase_tables.tbl and get the list of table names
        tableNames = new ArrayList<>();
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
	String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_tables.tbl";
	File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            System.out.println("Table Metadata Information has been deleted. Cannot guarantee proper execution.");
            throw new MissingTableFileException("davisbase_tables");
        }else;
        
        RandomAccessFile catalogTableFile = new RandomAccessFile(absoluteFilePath, "rw");
	if(catalogTableFile.length() < DavisBase.PAGESIZE) //no meta data information found
            throw new InvalidTableInformationException("davisbase_tables");
        else;
        
        int curPage = 1;
        while(curPage != -1){
            long pageStart = (curPage-1)*DavisBase.PAGESIZE;
            catalogTableFile.seek(pageStart);
            catalogTableFile.skipBytes(1); //unused- will be page type
            int numRecordsInPage = catalogTableFile.readByte();
            catalogTableFile.skipBytes(2);//unused will be start of cell area
            int nextPage = catalogTableFile.readInt();
            ArrayList<Short> cellLocations = new ArrayList<>();
            for(int i=0;i<numRecordsInPage;++i)
                cellLocations.add(catalogTableFile.readShort());
            
            for(Short cellLocation:cellLocations){
                catalogTableFile.seek(pageStart+cellLocation);
                
                catalogTableFile.skipBytes(7);//skip over header and number of columns - will be 3 (name, record_count,root_page)
                int tableNameLen = catalogTableFile.readByte()-12;//read value - 0x0c
                //table names will always be non null
                catalogTableFile.skipBytes(2);//skip over serial codes for record_count and root_page
                
                //read name of table
                byte[] tabNameByteBuffer = new byte[tableNameLen];
                catalogTableFile.read(tabNameByteBuffer);
                String tabName = new String(tabNameByteBuffer);
                tableNames.add(tabName);
            }
            
            curPage = nextPage;
        }
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
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     */
    public static ArrayList<String> getTableColumns(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        return getTableInfo(tableName).colNames;
    }

    /**
     * Returns schema of given table by reading davisbase_columns.tbl file
     * @param tableName Name of table
     * @return TableColumnInfo object describing schema of table
     * @throws MissingTableFileException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     */
    public static TableColumnInfo getTableInfo(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        TableColumnInfo tci = new TableColumnInfo();
        int numCols = 0;
        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<DataType> colDataTypes = new ArrayList<>();
        ArrayList<Boolean> nullable = new ArrayList<>();
        
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
	String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_columns.tbl";
	File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            System.out.println("Table Metadata Information has been deleted. Cannot guarantee proper execution.");
            throw new MissingTableFileException("davisbase_columns");
        }else;
        
        RandomAccessFile catalogTableFile = new RandomAccessFile(absoluteFilePath, "rw");
	if(catalogTableFile.length() < DavisBase.PAGESIZE) //no meta data information found
            throw new InvalidTableInformationException("davisbase_columns");
        else;
        
        int curPage = 1;
        while(curPage != -1){
            long pageStart = (curPage-1)*DavisBase.PAGESIZE;
            catalogTableFile.seek(pageStart);
            catalogTableFile.skipBytes(1); //unused- will be page type
            int numRecordsInPage = catalogTableFile.readByte();
            catalogTableFile.skipBytes(2);//unused will be start of cell area
            int nextPage = catalogTableFile.readInt();
            ArrayList<Short> cellLocations = new ArrayList<>();
            for(int i=0;i<numRecordsInPage;++i)
                cellLocations.add(catalogTableFile.readShort());
            
            for(Short cellLocation:cellLocations){
                catalogTableFile.seek(pageStart+cellLocation);
                
                catalogTableFile.skipBytes(7);//skip over header and number of columns - will be 5 (table_name, column_name, data_type, ordinal_position, is_nullable)
                int tableNameLen = catalogTableFile.readByte()-12;//read value - 0x0c
                //table names will always be non null
                int colNameLen = catalogTableFile.readByte()-12;//read value - 0x0c
                //column names will always be non null
                int dataTypeLen = catalogTableFile.readByte()-12;//read value - 0x0c
                catalogTableFile.skipBytes(1);//skip over size of ordinal_position
                int nullabilityLen = catalogTableFile.readByte()-12;//read value - 0x0c
                
                //read name of table
                byte[] tabNameByteBuffer = new byte[tableNameLen];
                catalogTableFile.read(tabNameByteBuffer);
                String tabName = new String(tabNameByteBuffer);
                if(!tabName.equalsIgnoreCase(tableName))
                    continue;// this is not the table you are looking for
                else
                    ++numCols;
                byte[] colNameByteBuffer = new byte[colNameLen];
                catalogTableFile.read(colNameByteBuffer);
                String colName = new String(colNameByteBuffer);
                colNames.add(colName);
                byte[] dataTypeByteBuffer = new byte[dataTypeLen];
                catalogTableFile.read(dataTypeByteBuffer);
                String dataTypeName = new String(dataTypeByteBuffer);
                colDataTypes.add(new DataType(dataTypeName));
                //ordinal position is ignored - assumed to always be increasing
                catalogTableFile.skipBytes(1);
                if(nullabilityLen==2)//NO
                    nullable.add(false);
                else
                    nullable.add(true);
            }
            
            curPage = nextPage;
        }
        
        tci.numCols = numCols;
        tci.colDataTypes = colDataTypes;
        tci.colNames = colNames;
        tci.colNullable = nullable;
        return tci;
    }
    
    //Variable declarations
    /**< Set to true to indicate program must terminate.*/public static boolean exitFlag = false;
    /**< Variable attached to STDIN to read user inputs, delimited by ;*/static Scanner inputStream = new Scanner(System.in).useDelimiter(";");
    /**< Name of currently selected database.*/public static String activeDatabase = "";//null - use this if implementing databases feature
    public static ArrayList<String> tableNames;
    
    /**< Variable holding the text string displayed as prompt. Terminated with >.*/static String promptText = "davisql>";
    /**< String describing version number.*/public static final String VERSIONSTRING = "0.1.0";
    /**< Copyright message.*/static final String COPYRIGHTSTRING = "Copyright (c) 2018, Sandeep Sukumaran. All rights reserved.";
    /**< Page size used by database application.*/ public static final long PAGESIZE = 512;
}
