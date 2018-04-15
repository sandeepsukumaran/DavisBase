/*
 * Copyright (C) 2018 Sandeep
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.github.sandeepsukumaran.davisbase.tree;

import io.github.sandeepsukumaran.davisbase.datatype.DataType;
import io.github.sandeepsukumaran.davisbase.exception.FileAccessException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidDataType;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.result.ResultSet;
import io.github.sandeepsukumaran.davisbase.result.ResultSetRow;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 *
 * @author Sandeep
 */
public class ReadRows {
    public ReadRows(){}

    /**
     * Read and return all rows and columns from table.
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @return ResultSet of data read.
     * @throws io.github.sandeepsukumaran.davisbase.exception.FileAccessException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     */
    public static ResultSet readRows(String tableName) throws FileAccessException, MissingTableFileException{
        ResultSet rs = new ResultSet();
        try{
            TableColumnInfo schema = getTableColumnInfoFromMetadata(tableName);
        
        
            String workingDirectory = System.getProperty("user.dir"); // gets current working directory
            String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + tableName+".tbl";
            File file = new File(absoluteFilePath);
            if (!(file.exists() && !file.isDirectory())){
                throw new MissingTableFileException(tableName);
            }else;
        
            RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
            if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
                throw new InvalidTableInformationException(tableName);
            else;
            
            int curPage = 1;
            while (curPage != -1){
                long pageStart = (curPage-1)*DavisBase.PAGESIZE;
                tableFile.seek(pageStart);
                tableFile.skipBytes(1); //unused- will be page type
                int numRecordsInPage = tableFile.readByte();
                tableFile.skipBytes(2);//unused will be start of cell area
                int nextPage = tableFile.readInt();
                ArrayList<Short> cellLocations = new ArrayList<>();
                for(int i=0;i<numRecordsInPage;++i)
                    cellLocations.add(tableFile.readShort());
                
                for(Short cellLocation:cellLocations){
                    tableFile.seek(pageStart+cellLocation);
                    
                    tableFile.skipBytes(2);//skip over payload size
                    int row_id = tableFile.readInt();
                    tableFile.skipBytes(1);//skip over number of columns
                    ArrayList<Boolean> isnull = new ArrayList<>();
                    HashMap<Integer,Integer> textFieldLength = new HashMap<>();
                    for(int col=0;col<schema.numCols;++col){
                        int serialTypeCode = tableFile.readByte();
                        if (serialTypeCode<4)
                            isnull.add(true);
                        else if (serialTypeCode==12)
                            isnull.add(true);
                        else
                            isnull.add(false);
                        if (serialTypeCode>12)
                            textFieldLength.put(col, serialTypeCode - 12);
                    }
                    
                    //read record data
                    ResultSetRow rsr = new ResultSetRow();
                    for(int col=0;col<schema.numCols;++col){
                        switch(schema.colDataTypes.get(col).getDataTypeAsInt()){
                            case 1://TINYINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(1);
                                }else{
                                    int val = tableFile.readByte();
                                    rsr.contents.add(DataType.dataAsString(1,val));
                                }
                                break;
                            case 2://SMALLINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(2);
                                }else{
                                    int val = tableFile.readShort();
                                    rsr.contents.add(DataType.dataAsString(2,val));
                                }
                            case 3://INT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(4);
                                }else{
                                    int val = tableFile.readInt();
                                    rsr.contents.add(DataType.dataAsString(3,val));
                                }
                            case 4://BIGINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(4,val));
                                }
                            case 5://REAL
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(4);
                                }else{
                                    float val = tableFile.readFloat();
                                    rsr.contents.add(DataType.dataAsString(5,val));
                                }
                            case 6://DOUBLE
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    double val = tableFile.readDouble();
                                    rsr.contents.add(DataType.dataAsString(6,val));
                                }
                            case 7://DATETIME
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(7,val));
                                }
                            case 8://DATE
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(8,val));
                                }
                            case 9://TEXT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(1);
                                }else{
                                    byte[] textBuffer = new byte[textFieldLength.get(col)];
                                    tableFile.read(textBuffer);
                                    rsr.contents.add(new String(textBuffer));
                                }
                        }
                    }
                    
                    //all data in a single record read
                    rs.data.add(rsr);
                }
                curPage = nextPage;
            }
        }catch(InvalidTableInformationException | IOException e){throw new FileAccessException();
        }catch(MissingTableFileException e){throw e;}
        return rs;
    }

    /**
     * Read and return rows from table
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @param colName Name of column to be used for WHERE clause.
     * @param operator Comparison operator <,=,<=,>,>=,<>
     * @param tarValue Value to be compared against.
     * @return ResultSet of data read
     * @throws NoSuchColumnException
     * @throws io.github.sandeepsukumaran.davisbase.exception.FileAccessException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidDataType
     */
    public static ResultSet readRows(String tableName, String colName, String operator, String tarValue) throws NoSuchColumnException, FileAccessException, MissingTableFileException, InvalidDataType{
        ResultSet rs = new ResultSet();
        try{
            TableColumnInfo schema = getTableColumnInfoFromMetadata(tableName);
        
            int colNum = schema.colNames.indexOf(colName);
            if(colNum==-1)
                throw new NoSuchColumnException(colName, tableName);
            else;
        
            String workingDirectory = System.getProperty("user.dir"); // gets current working directory
            String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + tableName+".tbl";
            File file = new File(absoluteFilePath);
            if (!(file.exists() && !file.isDirectory())){
                throw new MissingTableFileException(tableName);
            }else;
        
            RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
            if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
                throw new InvalidTableInformationException(tableName);
            else;
            
            Object targetValue=null;
            switch(schema.colDataTypes.get(colNum).getDataTypeAsInt()){
                case 1://tinyint
                case 2://smallint
                    targetValue = Short.parseShort(tarValue);
                    break;
                case 3://int
                    targetValue = Integer.parseInt(tarValue);
                    break;
                case 4://bigint
                case 7://datetime
                case 8://date
                    targetValue = Long.parseLong(tarValue);
                    break;
                case 5://real
                    targetValue = Float.parseFloat(tarValue);
                    break;
                case 6://double
                    targetValue = Double.parseDouble(tarValue);
                    break;
                case 9:
                    targetValue = tarValue;       
            }
            
            int curPage = 1;
            while (curPage != -1){
                long pageStart = (curPage-1)*DavisBase.PAGESIZE;
                tableFile.seek(pageStart);
                tableFile.skipBytes(1); //unused- will be page type
                int numRecordsInPage = tableFile.readByte();
                tableFile.skipBytes(2);//unused will be start of cell area
                int nextPage = tableFile.readInt();
                ArrayList<Short> cellLocations = new ArrayList<>();
                for(int i=0;i<numRecordsInPage;++i)
                    cellLocations.add(tableFile.readShort());
                
                for(Short cellLocation:cellLocations){
                    boolean useThisRecord = true;
                    tableFile.seek(pageStart+cellLocation);
                    
                    tableFile.skipBytes(2);//skip over payload size
                    int row_id = tableFile.readInt();
                    tableFile.skipBytes(1);//skip over number of columns
                    ArrayList<Boolean> isnull = new ArrayList<>();
                    HashMap<Integer,Integer> textFieldLength = new HashMap<>();
                    for(int col=0;col<schema.numCols;++col){
                        int serialTypeCode = tableFile.readByte();
                        if (serialTypeCode<4)
                            isnull.add(true);
                        else if (serialTypeCode==12)
                            isnull.add(true);
                        else
                            isnull.add(false);
                        if (serialTypeCode>12)
                            textFieldLength.put(col, serialTypeCode - 12);
                    }
                    
                    if(isnull.get(colNum)) //current record holds a null value for required column
                        continue;
                    else;
                    
                    //read record data
                    ResultSetRow rsr = new ResultSetRow();
                    for(int col=0;useThisRecord && col<schema.numCols;++col){
                        switch(schema.colDataTypes.get(col).getDataTypeAsInt()){
                            case 1://TINYINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(1);
                                }else{
                                    int val = tableFile.readByte();
                                    rsr.contents.add(DataType.dataAsString(1,val));
                                    if((col==colNum)&&(!evaluate("tinyint",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                                break;
                            case 2://SMALLINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(2);
                                }else{
                                    int val = tableFile.readShort();
                                    rsr.contents.add(DataType.dataAsString(2,val));
                                    if((col==colNum)&&(!evaluate("smallint",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 3://INT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(4);
                                }else{
                                    int val = tableFile.readInt();
                                    rsr.contents.add(DataType.dataAsString(3,val));
                                    if((col==colNum)&&(!evaluate("int",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 4://BIGINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(4,val));
                                    if((col==colNum)&&(!evaluate("bigint",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 5://REAL
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(4);
                                }else{
                                    float val = tableFile.readFloat();
                                    rsr.contents.add(DataType.dataAsString(5,val));
                                    if((col==colNum)&&(!evaluate("real",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 6://DOUBLE
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    double val = tableFile.readDouble();
                                    rsr.contents.add(DataType.dataAsString(6,val));
                                    if((col==colNum)&&(!evaluate("double",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 7://DATETIME
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(7,val));
                                    if((col==colNum)&&(!evaluate("datetime",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 8://DATE
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(8,val));
                                    if((col==colNum)&&(!evaluate("date",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                            case 9://TEXT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(1);
                                }else{
                                    byte[] textBuffer = new byte[textFieldLength.get(col)];
                                    tableFile.read(textBuffer);
                                    String val = new String(textBuffer);
                                    rsr.contents.add(val);
                                    if((col==colNum)&&(!evaluate("text",val,targetValue,operator)))
                                        useThisRecord = false;
                                    else;
                                }
                        }
                    }
                    
                    //all data in a single record read
                    if(useThisRecord)
                        rs.data.add(rsr);
                    else;
                }
                curPage = nextPage;
            }
        }catch(InvalidTableInformationException | IOException e){throw new FileAccessException();
        }catch(MissingTableFileException e){throw e;
        }catch(NumberFormatException e){throw new InvalidDataType(tarValue);}
        return rs;
    }
    
    /**
     * Read and return rows from table
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @param colName Name of column to be used for WHERE clause.
     * @param tarisnull Check for specified column IS NULL if true, IS NOT NULL if false.
     * @return ResultSet of data read
     * @throws NoSuchColumnException
     * @throws io.github.sandeepsukumaran.davisbase.exception.FileAccessException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     */
    public static ResultSet readRows(String tableName, String colName, Boolean tarisnull) throws NoSuchColumnException, FileAccessException, MissingTableFileException{
        ResultSet rs = new ResultSet();
        try{
            TableColumnInfo schema = getTableColumnInfoFromMetadata(tableName);
        
            int colNum = schema.colNames.indexOf(colName);
            if(colNum==-1)
                throw new NoSuchColumnException(colName, tableName);
            else;
        
            String workingDirectory = System.getProperty("user.dir"); // gets current working directory
            String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + tableName+".tbl";
            File file = new File(absoluteFilePath);
            if (!(file.exists() && !file.isDirectory())){
                throw new MissingTableFileException(tableName);
            }else;
        
            RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
            if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
                throw new InvalidTableInformationException(tableName);
            else;
            
            int curPage = 1;
            while (curPage != -1){
                long pageStart = (curPage-1)*DavisBase.PAGESIZE;
                tableFile.seek(pageStart);
                tableFile.skipBytes(1); //unused- will be page type
                int numRecordsInPage = tableFile.readByte();
                tableFile.skipBytes(2);//unused will be start of cell area
                int nextPage = tableFile.readInt();
                ArrayList<Short> cellLocations = new ArrayList<>();
                for(int i=0;i<numRecordsInPage;++i)
                    cellLocations.add(tableFile.readShort());
                
                for(Short cellLocation:cellLocations){
                    tableFile.seek(pageStart+cellLocation);
                    
                    tableFile.skipBytes(2);//skip over payload size
                    int row_id = tableFile.readInt();
                    tableFile.skipBytes(1);//skip over number of columns
                    ArrayList<Boolean> isnull = new ArrayList<>();
                    HashMap<Integer,Integer> textFieldLength = new HashMap<>();
                    for(int col=0;col<schema.numCols;++col){
                        int serialTypeCode = tableFile.readByte();
                        if (serialTypeCode<4)
                            isnull.add(true);
                        else if (serialTypeCode==12)
                            isnull.add(true);
                        else
                            isnull.add(false);
                        if (serialTypeCode>12)
                            textFieldLength.put(col, serialTypeCode - 12);
                    }
                    
                    if(!Objects.equals(isnull.get(colNum), tarisnull)) //current record isnull value does not match required isnull
                        continue;
                    else;
                    
                    //read record data
                    ResultSetRow rsr = new ResultSetRow();
                    for(int col=0;col<schema.numCols;++col){
                        switch(schema.colDataTypes.get(col).getDataTypeAsInt()){
                            case 1://TINYINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(1);
                                }else{
                                    int val = tableFile.readByte();
                                    rsr.contents.add(DataType.dataAsString(1,val));
                                }
                                break;
                            case 2://SMALLINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(2);
                                }else{
                                    int val = tableFile.readShort();
                                    rsr.contents.add(DataType.dataAsString(2,val));
                                }
                            case 3://INT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(4);
                                }else{
                                    int val = tableFile.readInt();
                                    rsr.contents.add(DataType.dataAsString(3,val));
                                }
                            case 4://BIGINT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(4,val));
                                }
                            case 5://REAL
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(4);
                                }else{
                                    float val = tableFile.readFloat();
                                    rsr.contents.add(DataType.dataAsString(5,val));
                                }
                            case 6://DOUBLE
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    double val = tableFile.readDouble();
                                    rsr.contents.add(DataType.dataAsString(6,val));
                                }
                            case 7://DATETIME
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(7,val));
                                }
                            case 8://DATE
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(8);
                                }else{
                                    long val = tableFile.readLong();
                                    rsr.contents.add(DataType.dataAsString(8,val));
                                }
                            case 9://TEXT
                                if(isnull.get(col)){
                                    rsr.contents.add("NULL");
                                    tableFile.skipBytes(1);
                                }else{
                                    byte[] textBuffer = new byte[textFieldLength.get(col)];
                                    tableFile.read(textBuffer);
                                    String val = new String(textBuffer);
                                    rsr.contents.add(val);
                                }
                        }
                    }
                    
                    //all data in a single record read
                    rs.data.add(rsr);
                }
                curPage = nextPage;
            }
        }catch(InvalidTableInformationException | IOException e){throw new FileAccessException();
        }catch(MissingTableFileException e){throw e;}
        return rs;
    }
    
    /**
     *
     * @param tableName
     * @return TableInfo object containing information about table.
     * @throws MissingTableFileException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     */
    public static TableInfo getTableInfoFromMetadata(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        TableInfo ti=null;
        /*
        Read all rows from davisbase_tables.tbl. If the first page is leaf, great. Else go to first leaf
        and read all rows sequentially. Each record starts with a header where the first field is the size
        of the header in int.
        */
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
                
                catalogTableFile.skipBytes(2);
                int rowid = catalogTableFile.readInt();
                catalogTableFile.skipBytes(1);//skip over number of columns - will be 3 (name, record_count,root_page)
                int tableNameLen = catalogTableFile.readByte()-12;//read value - 0x0c
                //table names will always be non null
                catalogTableFile.skipBytes(2);//skip over serial codes for record_count and root_page
                
                //read name of table
                byte[] tabNameByteBuffer = new byte[tableNameLen];
                catalogTableFile.read(tabNameByteBuffer);
                String tabName = new String(tabNameByteBuffer);
                if(!tableName.equalsIgnoreCase(tabName))
                    continue;//*slow wave*This is not the table you are looking for
                else;
                int record_count = catalogTableFile.readInt();
                int root_page = catalogTableFile.readShort();
                
                return new TableInfo(rowid,tabName,record_count,root_page);
            }
            
            curPage = nextPage;
        }
        return ti;
    }

    /**
     * Return Information about the schema of a table.
     * @param tableName Name of table guaranteed to exist in database.
     * @return Information about schema of a table in a TableColumnInfo object.
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     */
    public static TableColumnInfo getTableColumnInfoFromMetadata(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        TableColumnInfo tci = new TableColumnInfo();
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
	String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_tables.tbl";
	File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            System.out.println("Table Metadata Information has been deleted. Cannot guarantee proper execution.");
            throw new MissingTableFileException("davisbase_columns");
        }else;
        
        RandomAccessFile catalogTableColumnFile = new RandomAccessFile(absoluteFilePath, "rw");
	if(catalogTableColumnFile.length() < DavisBase.PAGESIZE) // to handle exception thrown when davisbase_tables.tbl is newly created and has no data to execute readByte operation
            throw new InvalidTableInformationException("davisbase_columns");
        else;
        
        /*
            Go through all leaf pages and get all info about the table
        */
        int curPage = 1;
        while(curPage != -1){
            long pageStart = (curPage-1)*DavisBase.PAGESIZE;
            catalogTableColumnFile.seek(pageStart);
            catalogTableColumnFile.skipBytes(1); //unused- will be page type
            int numRecordsInPage = catalogTableColumnFile.readByte();
            catalogTableColumnFile.skipBytes(2);//unused will be start of cell area
            int nextPage = catalogTableColumnFile.readInt();
            ArrayList<Short> cellLocations = new ArrayList<>();
            for(int i=0;i<numRecordsInPage;++i)
                cellLocations.add(catalogTableColumnFile.readShort());
            
            for(Short cellLocation:cellLocations){
                catalogTableColumnFile.seek(pageStart+cellLocation);
                
                catalogTableColumnFile.skipBytes(2);
                int rowid = catalogTableColumnFile.readInt();
                catalogTableColumnFile.skipBytes(1);//skip over number of columns
                //^- will be 5 (tablename,columnname,datatype,ordinal_position,is_nullable)
                
                int tableNameLen = catalogTableColumnFile.readByte()-12;//read value - 0x0c
                int columnNameLen = catalogTableColumnFile.readByte()-12;//read value - 0x0c
                int dataTypeLen = catalogTableColumnFile.readByte()-12;//read value - 0x0c
                catalogTableColumnFile.skipBytes(1);
                int nullableLen = catalogTableColumnFile.readByte()-12;//read value - 0x0c
                //all text strings will be non-nullable
                
                //read name of table
                byte[] tabNameByteBuffer = new byte[tableNameLen];
                catalogTableColumnFile.read(tabNameByteBuffer);
                String tabName = new String(tabNameByteBuffer);
                if(!tableName.equalsIgnoreCase(tabName))
                    continue;//*slow wave*This is not the table you are looking for
                else;
                //read column name
                byte[] colNameByteBuffer = new byte[columnNameLen];
                catalogTableColumnFile.read(colNameByteBuffer);
                String colName = new String(colNameByteBuffer);
                //read datatype
                byte[] dataTypeByteBuffer = new byte[dataTypeLen];
                catalogTableColumnFile.read(dataTypeByteBuffer);
                String dataType = new String(dataTypeByteBuffer);
                //read ordinal position
                int ordinal_pos = catalogTableColumnFile.readShort();
                //skip over nullability - can be decided based on length of string
                catalogTableColumnFile.skipBytes(nullableLen);
                
                //add data to return object
                tci.colNames.add(ordinal_pos,colName);
                tci.colDataTypes.add(ordinal_pos,new DataType(dataType));
                if(nullableLen==3)
                    tci.colNullable.add(ordinal_pos,true);
                else
                    tci.colNullable.add(ordinal_pos,false);
            }
            
            curPage = nextPage;
        }
        
        tci.numCols = tci.colNames.size();
        return tci;
    }

    private static boolean evaluate(String dtype, Object curVal,Object tarVal,String op){
        switch(DataType.getDataTypeAsInt(dtype)){
            case 1://tinyint
            case 2://smallint
                short scurval = (Short)curVal;
                short starval = (Short)tarVal;
                switch(op){
                    case "<": return scurval<starval;
                    case "<=": return scurval<=starval;
                    case ">": return scurval>starval;
                    case ">=": return scurval>=starval;
                    case "=": return scurval==starval;
                    case "<>": return scurval!=starval;
                }
                break;
            case 3://int
                int icurval = (Integer)curVal;
                int itarval = (Integer)tarVal;
                switch(op){
                    case "<": return icurval<itarval;
                    case "<=": return icurval<=itarval;
                    case ">": return icurval>itarval;
                    case ">=": return icurval>=itarval;
                    case "=": return icurval==itarval;
                    case "<>": return icurval!=itarval;
                }
                break;
            case 4://bigint
                long lcurval = (Long)curVal;
                long ltarval = (Long)tarVal;
                switch(op){
                    case "<": return lcurval<ltarval;
                    case "<=": return lcurval<=ltarval;
                    case ">": return lcurval>ltarval;
                    case ">=": return lcurval>=ltarval;
                    case "=": return lcurval==ltarval;
                    case "<>": return lcurval!=ltarval;
                }
                break;
            case 5://real
                float fcurval = (Float)curVal;
                float ftarval = (Float)tarVal;
                switch(op){
                    case "<": return fcurval<ftarval;
                    case "<=": return fcurval<=ftarval;
                    case ">": return fcurval>ftarval;
                    case ">=": return fcurval>=ftarval;
                    case "=": return fcurval==ftarval;
                    case "<>": return fcurval!=ftarval;
                }
                break;
            case 6://double
                double dcurval = (Double)curVal;
                double dtarval = (Double)tarVal;
                switch(op){
                    case "<": return dcurval<dtarval;
                    case "<=": return dcurval<=dtarval;
                    case ">": return dcurval>dtarval;
                    case ">=": return dcurval>=dtarval;
                    case "=": return dcurval==dtarval;
                    case "<>": return dcurval!=dtarval;
                }
                break;
            case 7://datetime
                long dttcurval = (Long)curVal;
                long dtttarval = (Long)tarVal;
                switch(op){
                    case "<": return dttcurval<dtttarval;
                    case "<=": return dttcurval<=dtttarval;
                    case ">": return dttcurval>dtttarval;
                    case ">=": return dttcurval>=dtttarval;
                    case "=": return dttcurval==dtttarval;
                    case "<>": return dttcurval!=dtttarval;
                }
                break;
            case 8://date
                long dtcurval = (Long)curVal;
                long dttarval = (Long)tarVal;
                switch(op){
                    case "<": return dtcurval<dttarval;
                    case "<=": return dtcurval<=dttarval;
                    case ">": return dtcurval>dttarval;
                    case ">=": return dtcurval>=dttarval;
                    case "=": return dtcurval==dttarval;
                    case "<>": return dtcurval!=dttarval;
                }
                break;
            case 9://text
                String stcurval = (String)curVal;
                String sttarval = (String)tarVal;
                switch(op){
                    case "<": return stcurval.compareTo(sttarval)<0;
                    case "<=": return stcurval.compareTo(sttarval)<=0;
                    case ">": return stcurval.compareTo(sttarval)>0;
                    case ">=": return stcurval.compareTo(sttarval)>=0;
                    case "=": return stcurval.equals(sttarval);
                    case "<>": return !stcurval.equals(sttarval);
                }
                break;
        }
        return false;
    }
}
