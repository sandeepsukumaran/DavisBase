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

import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.tree.UpdateRecord;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

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
}
