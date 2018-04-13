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
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.result.ResultSet;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

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
     */
    public static ResultSet readRows(String tableName){
        ResultSet rs = null;return rs;
    }

    /**
     * Read and return rows from table
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @param colName Name of column to be used for WHERE clause.
     * @param operator Comparison operator <,=,<=,>,>=,<>
     * @return ResultSet of data read
     * @throws NoSuchColumnException
     */
    public static ResultSet readRows(String tableName, String colName, String operator) throws NoSuchColumnException{
        ResultSet rs = null;return rs;
    }
    
    /**
     * Read and return rows from table
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @param colName Name of column to be used for WHERE clause.
     * @param isnull Check for specified column IS NULL if true, IS NOT NULL if false.
     * @return ResultSet of data read
     * @throws NoSuchColumnException
     */
    public static ResultSet readRows(String tableName, String colName, Boolean isnull) throws NoSuchColumnException{
        ResultSet rs = null;return rs;
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
            long pageStart = curPage*DavisBase.PAGESIZE;
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
                int tableNameLen = catalogTableFile.readByte()-13;//read value - 0x0c
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
            long pageStart = curPage*DavisBase.PAGESIZE;
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
                
                int tableNameLen = catalogTableColumnFile.readByte()-13;//read value - 0x0c
                int columnNameLen = catalogTableColumnFile.readByte()-13;//read value - 0x0c
                int dataTypeLen = catalogTableColumnFile.readByte()-13;//read value - 0x0c
                catalogTableColumnFile.skipBytes(1);
                int nullableLen = catalogTableColumnFile.readByte()-13;//read value - 0x0c
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
                //read nullability
                byte[] nullableByteBuffer = new byte[nullableLen];
                catalogTableColumnFile.read(nullableByteBuffer);
                String nullable = new String(nullableByteBuffer);
                
                //add data to return object
                tci.colNames.add(ordinal_pos,colName);
                tci.colDataTypes.add(ordinal_pos,new DataType(dataType));
                if(nullable.equalsIgnoreCase("yes"))
                    tci.colNullable.add(ordinal_pos,true);
                else
                    tci.colNullable.add(ordinal_pos,false);
            }
            
            curPage = nextPage;
        }
        
        tci.numCols = tci.colNames.size();
        return tci;
    }
}
