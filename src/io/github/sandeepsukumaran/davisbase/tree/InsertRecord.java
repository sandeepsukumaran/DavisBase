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

import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author Sandeep
 */
public class InsertRecord {
    public InsertRecord(){}
    
    /**
     *
     * @param tableName
     * @param record
     * @param row_id
     * @throws MissingTableFileException
     * @throws InvalidTableInformationException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void writeRecordToFile(String tableName,byte[] record,int row_id) throws MissingTableFileException, InvalidTableInformationException, FileNotFoundException, IOException{
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
        String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + tableName+".tbl";
        File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            throw new MissingTableFileException(tableName);
        }else;

        RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");

        if(tableFile.length() < DavisBase.PAGESIZE) //invalid file
            throw new InvalidTableInformationException(tableName);
        else;

        int numPages = (int)(tableFile.length()/DavisBase.PAGESIZE);
        if(numPages==1)
            if(writeRecordToFirstPage(tableFile,record,row_id,tableName))
                UpdateRecord.setRootPage(tableName);
            else{}
        else
            writeRecordToPage(tableFile,record,numPages,row_id,tableName);

        tableFile.close();
    }
    
    private static boolean writeRecordToFirstPage(RandomAccessFile tableFile, byte[] record,int row_id,String tableName) throws IOException, MissingTableFileException, FileNotFoundException, InvalidTableInformationException{
        tableFile.seek(0);
        tableFile.skipBytes(1);//skip over page type - will be0x0d
        short numCols = tableFile.readByte();
        short cellStart = tableFile.readShort();
        int headerEnd = 8+2*numCols;
        boolean returnval;
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
            returnval = false;
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
            tableFile.writeShort((short)(2*DavisBase.PAGESIZE - record.length - DavisBase.PAGESIZE));
            tableFile.writeInt(-1);
            tableFile.writeShort((short)(2*DavisBase.PAGESIZE - record.length - DavisBase.PAGESIZE));
            tableFile.seek(2*DavisBase.PAGESIZE - record.length);
            tableFile.write(record);
            //write parent page
            tableFile.seek(2*DavisBase.PAGESIZE);
            tableFile.writeByte(5);//0x05
            tableFile.writeByte(1);
            tableFile.writeShort((short)(3*DavisBase.PAGESIZE - 8 - 2*DavisBase.PAGESIZE));
            tableFile.writeInt(2);
            tableFile.writeShort((short)(3*DavisBase.PAGESIZE - 8 - 2*DavisBase.PAGESIZE));
            tableFile.seek(3*DavisBase.PAGESIZE - 8);
            tableFile.writeInt(1);
            tableFile.writeInt(row_id);
            returnval = true;
        }
        UpdateRecord.incrementRecordCount(tableName);
        return returnval;
    }
    
    private static void writeRecordToPage(RandomAccessFile tableFile, byte[] record, int pageNum,int row_id,String tableName) throws IOException, MissingTableFileException, FileNotFoundException, InvalidTableInformationException{
        if(pageNum==3)
            pageNum=2;
        else;
        
        tableFile.seek((pageNum-1)*DavisBase.PAGESIZE);
        tableFile.skipBytes(1);//skip over page type - will be0x0d
        byte numCols = tableFile.readByte();
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
            tableFile.writeShort((short)(curPage*DavisBase.PAGESIZE - record.length - (curPage-1)*DavisBase.PAGESIZE));
            tableFile.writeInt(-1);
            tableFile.writeShort((short)(curPage*DavisBase.PAGESIZE - record.length - (curPage-1)*DavisBase.PAGESIZE));
            tableFile.seek(curPage*DavisBase.PAGESIZE - record.length);
            tableFile.write(record);
            //write parent page
            tableFile.seek(2*DavisBase.PAGESIZE);
            tableFile.skipBytes(1);//0x05
            long fp = tableFile.getFilePointer();
            byte numcells = tableFile.readByte();
            tableFile.seek(fp);
            tableFile.writeByte(numcells+1);
            fp = tableFile.getFilePointer();
            short cellstart = tableFile.readShort();
            tableFile.seek(fp);
            tableFile.writeShort(cellstart-8);
            tableFile.writeInt(curPage);
            tableFile.skipBytes(2*numcells);
            tableFile.writeShort(cellstart-8);
            tableFile.seek(2*DavisBase.PAGESIZE+cellstart-8);
            tableFile.writeInt(pageNum);
            tableFile.writeInt(row_id);
        }
        UpdateRecord.incrementRecordCount(tableName);
    }
}
