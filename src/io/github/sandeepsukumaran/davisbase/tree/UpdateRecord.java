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
import java.util.ArrayList;

/**
 *
 * @author Sandeep
 */
public class UpdateRecord {
    public UpdateRecord(){}
    
    public static void incrementRecordCount(String tableName) throws MissingTableFileException, FileNotFoundException, InvalidTableInformationException, IOException{
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
        String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + "davisbase_tables.tbl";
        File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            throw new MissingTableFileException("davisbase_tables");
        }else;
        
        RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
        if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
            throw new InvalidTableInformationException(tableName);
        else;
        
        int curPage=1;
        while(curPage!=-1){
            long pageStart = curPage*DavisBase.PAGESIZE;
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
                tableFile.skipBytes(7);//skip over header+numColumns
                int nameLen = tableFile.readByte();
                tableFile.skipBytes(2);//skip over length of record_count and root_page
                byte[] b = new byte[nameLen];
                tableFile.read(b);
                String tblName = new String(b);
                if(!tblName.equals(tableName))
                    continue;
                else{}
                long pos = tableFile.getFilePointer();
                int curCount = tableFile.readInt();
                tableFile.seek(pos);
                tableFile.writeInt(curCount+1);
                return;
            }
            curPage = nextPage;
        }
    }
    public static void decrementRecordCount(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
        String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + "davisbase_tables.tbl";
        File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            throw new MissingTableFileException("davisbase_tables");
        }else;
        
        RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
        if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
            throw new InvalidTableInformationException(tableName);
        else;
        
        int curPage=1;
        while(curPage!=-1){
            long pageStart = curPage*DavisBase.PAGESIZE;
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
                tableFile.skipBytes(7);//skip over header+numColumns
                int nameLen = tableFile.readByte();
                tableFile.skipBytes(2);//skip over length of record_count and root_page
                byte[] b = new byte[nameLen];
                tableFile.read(b);
                String tblName = new String(b);
                if(!tblName.equals(tableName))
                    continue;
                else{}
                long pos = tableFile.getFilePointer();
                int curCount = tableFile.readInt();
                tableFile.seek(pos);
                tableFile.writeInt(curCount-1);
                return;
            }
            curPage = nextPage;
        }
    }
    public static void setRootPage(String tableName){
        
    }
}
