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
package io.github.sandeepsukumaran.davisbase.helpermethods;

import io.github.sandeepsukumaran.davisbase.datatype.DataType;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.query.createQueryHandler;
import static io.github.sandeepsukumaran.davisbase.query.createQueryHandler.TABLECOLMETADATACOLDATATYPES;
import static io.github.sandeepsukumaran.davisbase.query.createQueryHandler.TABLECOLMETADATACOLNAMES;
import static io.github.sandeepsukumaran.davisbase.query.createQueryHandler.TABLECOLMETADATACOLNULLABLE;
import static io.github.sandeepsukumaran.davisbase.query.createQueryHandler.TABLECOLMETADATANUMCOLS;
import io.github.sandeepsukumaran.davisbase.query.insertQueryHandler;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import io.github.sandeepsukumaran.davisbase.tree.UpdateRecord;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author Sandeep
 */
public class HelperMethods {
    public static ArrayList<Object> uniqueArrayList(ArrayList<Object> ip){
        ArrayList<Object> result = new ArrayList<>();
        for(Object e:ip)
            if(!result.contains(e))
                result.add(e);
        return result;
    }
    public static ArrayList<String> uniqueStringArrayList(ArrayList<String> ip){
        ArrayList<String> result = new ArrayList<>();
        for(String e:ip)
            if(!result.contains(e))
                result.add(e);
        return result;
    }
    
    public static boolean writeRecordToFirstPage(RandomAccessFile tableFile, byte[] record,int row_id) throws IOException, MissingTableFileException, FileNotFoundException, InvalidTableInformationException{
        tableFile.skipBytes(1);//skip over page type - will be0x0d
        short numCols = tableFile.readByte();
        short cellStart = tableFile.readShort();
        int headerEnd = 8+2*numCols;
        if(record.length+2 <= cellStart-headerEnd){
            //will fit in current page
            tableFile.seek(cellStart-record.length);
            tableFile.write(record);
            tableFile.seek(1);
            //write numCols
            tableFile.writeByte(numCols+1);
            //write cellStart
            tableFile.writeShort(cellStart-record.length);
            tableFile.skipBytes(4+2*numCols);
            //update cell pointers list - simple since row_id will always be monotonically increasing
            tableFile.writeShort(cellStart-record.length);
            return false;
        }else{
            //will not fit in current page
            //add two more pages
            tableFile.setLength(3*DavisBase.PAGESIZE);
            //update right sibling of first page
            tableFile.seek(4);
            tableFile.writeInt(2);
            //write second page data
            tableFile.seek(DavisBase.PAGESIZE);
            tableFile.writeByte(13);//0x0d
            tableFile.writeByte(1);
            tableFile.writeShort((short)(2*DavisBase.PAGESIZE - record.length));
            tableFile.writeInt(-1);
            tableFile.writeShort((short)(2*DavisBase.PAGESIZE - record.length));
            tableFile.seek(2*DavisBase.PAGESIZE - record.length);
            tableFile.write(record);
            //write parent page
            tableFile.seek(2*DavisBase.PAGESIZE);
            tableFile.writeByte(5);//0x05
            tableFile.writeByte(1);
            tableFile.writeShort((short)(3*DavisBase.PAGESIZE - 8));
            tableFile.writeInt(2);
            tableFile.writeShort((short)(3*DavisBase.PAGESIZE - 8));
            tableFile.seek(3*DavisBase.PAGESIZE - 8);
            tableFile.writeInt(1);
            tableFile.writeInt(row_id);
            return true;
        }
    }
    
    public static void writeRecordToPage(RandomAccessFile tableFile, byte[] record, int pageNum,int row_id) throws IOException, MissingTableFileException, FileNotFoundException, InvalidTableInformationException{
        if(pageNum==3)
            pageNum=2;
        else;
        
        tableFile.seek((pageNum-1)*DavisBase.PAGESIZE);
        tableFile.skipBytes(1);//skip over page type - will be0x0d
        short numCols = tableFile.readByte();
        short cellStart = tableFile.readShort();
        int headerEnd = 8+2*numCols;
        if(record.length+2 <= cellStart-headerEnd){
            //will fit in current page
            tableFile.seek((pageNum-1)*DavisBase.PAGESIZE + cellStart-record.length);
            tableFile.write(record);
            tableFile.seek((pageNum-1)*DavisBase.PAGESIZE+1);
            //write numCols
            tableFile.writeByte(numCols+1);
            //write cellStart
            tableFile.writeShort(cellStart-record.length);
            tableFile.skipBytes(4+2*numCols);
            //update cell pointers list - simple since row_id will always be monotonically increasing
            tableFile.writeShort(cellStart-record.length);
        }else{
            //will not fit in current page
            //add one more page
            tableFile.setLength(tableFile.length()+DavisBase.PAGESIZE);
            //update right sibling of current page
            tableFile.seek((pageNum-1)*DavisBase.PAGESIZE+4);
            int curPage;
            if(pageNum==2)
                curPage = 4;
            else
                curPage = pageNum+1;
            tableFile.writeInt(curPage);
            //write new page data
            tableFile.seek((curPage-1)*DavisBase.PAGESIZE);
            tableFile.writeByte(13);//0x0d
            tableFile.writeByte(1);
            tableFile.writeShort((short)(curPage*DavisBase.PAGESIZE - record.length));
            tableFile.writeInt(-1);
            tableFile.writeShort((short)(curPage*DavisBase.PAGESIZE - record.length));
            tableFile.seek(curPage*DavisBase.PAGESIZE - record.length);
            tableFile.write(record);
            //write parent page
            tableFile.seek(2*DavisBase.PAGESIZE);
            tableFile.skipBytes(1);//0x05
            long fp = tableFile.getFilePointer();
            int numcells = tableFile.readByte();
            tableFile.seek(fp);
            tableFile.writeByte(numcells+1);
            fp = tableFile.getFilePointer();
            int cellstart = tableFile.readShort();
            tableFile.seek(fp);
            tableFile.writeShort(cellstart-8);
            tableFile.writeInt(curPage);
            tableFile.skipBytes(2*numcells);
            tableFile.writeShort(cellstart-8);
            tableFile.seek(2*DavisBase.PAGESIZE+cellstart-8);
            tableFile.writeInt(pageNum);
            tableFile.writeInt(row_id);
        }
    }

    public static void writeInitialMetaDataFiles() throws MissingTableFileException, IOException, FileNotFoundException, InvalidTableInformationException{
        writeInitialMetaDataTablesFile();
        writeInitialMetaDataColumnsFile();
    }

    public static void writeInitialMetaDataTablesFile() throws MissingTableFileException, FileNotFoundException, IOException{
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
	String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_tables.tbl";
	File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            System.out.println("Table Metadata Information has been deleted. Cannot guarantee proper execution.");
            throw new MissingTableFileException("davisbase_tables");
        }else;
        
        RandomAccessFile catalogTableFile = new RandomAccessFile(absoluteFilePath, "rw");
        catalogTableFile.setLength(DavisBase.PAGESIZE);
        
        catalogTableFile.seek(0);
        catalogTableFile.writeByte(0x0d); //leaf page
        catalogTableFile.writeByte(2); //number of cells
        catalogTableFile.skipBytes(2); //cell start
        catalogTableFile.writeInt(-1); //right sibling
        
        ArrayList<Object> insertionData = new ArrayList<>();
        insertionData.add((Integer)(1));
        insertionData.add("davisbase_tables");
        insertionData.add((Integer)(2));
        insertionData.add((Short)((short)1));
        
        TableColumnInfo tci = new TableColumnInfo();
        tci.numCols = createQueryHandler.TABLEMETADATANUMCOLS;
        tci.colNames = new ArrayList<>(Arrays.asList(createQueryHandler.TABLEMETADATACOLNAMES));
        tci.colDataTypes = new ArrayList<>(Arrays.asList(createQueryHandler.TABLEMETADATACOLDATATYPES));
        tci.colNullable = new ArrayList<>(Arrays.asList(createQueryHandler.TABLEMETADATACOLNULLABLE));
        
        byte[] record = insertQueryHandler.buildRecord(insertionData, tci, tci.colNullable);
        catalogTableFile.seek(DavisBase.PAGESIZE - record.length);
        long pos = catalogTableFile.getFilePointer();
        catalogTableFile.seek(8);
        catalogTableFile.writeShort((short)pos);
        catalogTableFile.seek(pos);
        catalogTableFile.write(record);
        
        insertionData = new ArrayList<>();
        insertionData.add((Integer)(2));
        insertionData.add("davisbase_columns");
        insertionData.add((Integer)(10));
        insertionData.add((Short)((short)1));
        
        record = insertQueryHandler.buildRecord(insertionData, tci, tci.colNullable);
        catalogTableFile.seek(pos - record.length);
        long pos1 = catalogTableFile.getFilePointer();
        catalogTableFile.seek(10);
        catalogTableFile.writeShort((short)pos1);
        catalogTableFile.seek(2);
        catalogTableFile.writeShort((short)pos1);//update cell start
        catalogTableFile.seek(pos1);
        catalogTableFile.write(record);
        
        catalogTableFile.close();
    }
    
    public static void writeInitialMetaDataColumnsFile() throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
	String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_columns.tbl";
	File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            System.out.println("Table Metadata Information has been deleted. Cannot guarantee proper execution.");
            throw new MissingTableFileException("davisbase_columns");
        }else;
        
        RandomAccessFile catalogTableColFile = new RandomAccessFile(absoluteFilePath, "rw");
        catalogTableColFile.setLength(DavisBase.PAGESIZE);
        
        catalogTableColFile.seek(0);
        catalogTableColFile.writeByte(0x0d); //leaf page
        catalogTableColFile.writeByte(0x00); //number of cells
        catalogTableColFile.writeShort((short)DavisBase.PAGESIZE); //cell start
        catalogTableColFile.writeInt(-1); //right sibling
        
        //---------------------------------------------------------------------------------
        TableColumnInfo tci = new TableColumnInfo();
        tci.numCols = createQueryHandler.TABLECOLMETADATANUMCOLS;
        tci.colNames = new ArrayList<>(Arrays.asList(createQueryHandler.TABLECOLMETADATACOLNAMES));
        tci.colDataTypes = new ArrayList<>(Arrays.asList(createQueryHandler.TABLECOLMETADATACOLDATATYPES));
        tci.colNullable = new ArrayList<>(Arrays.asList(createQueryHandler.TABLECOLMETADATACOLNULLABLE));
        
        int highestrow_id=0; int curPage=1; int numPages;
        String tableName = "davisbase_tables";
        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<DataType> colDataType = new ArrayList<>();
        colNames.add("row_id"); colDataType.add(new DataType("int"));
        colNames.add("table_name"); colDataType.add(new DataType("text"));
        colNames.add("record_count"); colDataType.add(new DataType("int"));
        colNames.add("root_page"); colDataType.add(new DataType("smallint"));
        for(int i=0;i<colNames.size();++i){
            catalogTableColFile.seek(0);
            ArrayList<Object> insertionData = new ArrayList<>();
            insertionData.add((Integer)(highestrow_id+1+i));
            insertionData.add(tableName);
            insertionData.add(colNames.get(i));
            insertionData.add(colDataType.get(i).toString());
            insertionData.add((byte)(i+1));
            if(tci.colNullable.get(i))
                insertionData.add("YES");
            else
                insertionData.add("NO");
            
            byte[] record = insertQueryHandler.buildRecord(insertionData, tci, tci.colNullable);
            
            if(curPage==1)
                if(HelperMethods.writeRecordToFirstPage(catalogTableColFile, record, highestrow_id+1+i))
                    UpdateRecord.setRootPage("davisbase_columns");
                else{}
            else
                HelperMethods.writeRecordToPage(catalogTableColFile, record, curPage, highestrow_id+1+i);
            
            numPages = (int)(catalogTableColFile.length()/DavisBase.PAGESIZE);
            if(numPages==3)
                curPage=2;
            else
                curPage=numPages;
        }
        
        highestrow_id += colNames.size();
        tableName = "davisbase_columns";
        colNames = new ArrayList<>();
        colDataType = new ArrayList<>();
        colNames.add("row_id"); colDataType.add(new DataType("int"));
        colNames.add("table_name"); colDataType.add(new DataType("text"));
        colNames.add("column_name"); colDataType.add(new DataType("text"));
        colNames.add("data_type"); colDataType.add(new DataType("text"));
        colNames.add("ordinal_position"); colDataType.add(new DataType("tinyint"));
        colNames.add("is_nullable"); colDataType.add(new DataType("text"));
        for(int i=0;i<colNames.size();++i){
            catalogTableColFile.seek(0);
            ArrayList<Object> insertionData = new ArrayList<>();
            insertionData.add((Integer)(highestrow_id+1+i));
            insertionData.add(tableName);
            insertionData.add(colNames.get(i));
            insertionData.add(colDataType.get(i).toString());
            insertionData.add((byte)(i+1));
            if(tci.colNullable.get(i))
                insertionData.add("YES");
            else
                insertionData.add("NO");
            
            byte[] record = insertQueryHandler.buildRecord(insertionData, tci, tci.colNullable);
            
            if(curPage==1)
                if(HelperMethods.writeRecordToFirstPage(catalogTableColFile, record, highestrow_id+1+i))
                    UpdateRecord.setRootPage("davisbase_columns");
                else{}
            else
                HelperMethods.writeRecordToPage(catalogTableColFile, record, curPage, highestrow_id+1+i);
            
            numPages = (int)(catalogTableColFile.length()/DavisBase.PAGESIZE);
            if(numPages==3)
                curPage=2;
            else
                curPage=numPages;
        }
        
        catalogTableColFile.close();
    }
}
