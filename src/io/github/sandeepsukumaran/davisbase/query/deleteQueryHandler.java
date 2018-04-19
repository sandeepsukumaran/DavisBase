/*
 * Copyright (C) 2018 sandeep
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
package io.github.sandeepsukumaran.davisbase.query;

import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.tree.UpdateRecord;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author sandeep
 */
public class deleteQueryHandler {
    public deleteQueryHandler(String inputQuery){
        query = inputQuery;
        deletePattern = Pattern.compile(DELETE_QUERY);
        deleteMatcher = deletePattern.matcher(query);
        deleteMatcher.matches();
    }
    
    /**
     *
     * @throws NoSuchTableException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     * @throws java.io.IOException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     */
    public void execute() throws NoSuchTableException, MissingTableFileException, IOException, FileNotFoundException, InvalidTableInformationException{
        ArrayList<String> tableNames = DavisBase.getTableNames();
        String tableName = deleteMatcher.group("tablename");
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        int row_id = Integer.parseInt(deleteMatcher.group("rowid"));
        int count = deleteRowFromTable(tableName,row_id);
        if(count!=0)
            UpdateRecord.decrementRecordCount(tableName);//decrement by one variant
        else{}
        System.out.println("Query OK, "+count+" rows affected.");
    }
    
    private int deleteRowFromTable(String tableName, int row_id) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
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
        
        int curPage=1;
        boolean singleDeleteDone = false;
        while (curPage != -1){
            boolean page_dirty = false;
            long pageStart = (curPage-1)*DavisBase.PAGESIZE;
            tableFile.seek(pageStart);
            tableFile.skipBytes(1); //unused- will be page type
            byte numRecordsInPage = tableFile.readByte();
            tableFile.skipBytes(2);//unused will be start of cell area
            int nextPage = tableFile.readInt();
            ArrayList<Short> cellLocations = new ArrayList<>();
            for(byte i=0;i<numRecordsInPage;++i)
                cellLocations.add(tableFile.readShort());

            for(byte i=0;i<cellLocations.size();++i){
                Short cellLocation = cellLocations.get(i);
                tableFile.seek(pageStart+cellLocation);
                tableFile.skipBytes(2); //skip over size of payload
                int rid = tableFile.readInt();
                if(rid==row_id){
                    page_dirty = true;
                    cellLocations.remove(i);
                    break; //since row_id is primary key only one row will ever be affected.
                }else{}
            }
            if(page_dirty){
                tableFile.seek(pageStart+1);
                tableFile.writeByte((byte)cellLocations.size());
                tableFile.skipBytes(6);
                for(Short cellLocation:cellLocations)
                    tableFile.writeShort(cellLocation);
                singleDeleteDone = true;
            }else{}
            
            if(singleDeleteDone)
                return 1;
            else
                curPage = nextPage;
        }
        return 0;
    }

    private final Pattern deletePattern;
    private final Matcher deleteMatcher;
    private final String query;
    private final String DELETE_QUERY = "delete\\p{javaWhitespace}+from\\p{javaWhitespace}+table\\p{javaWhitespace}+(?<tablename>\\p{Alpha}\\w*)\\p{javaWhitespace}+where\\p{javaWhitespace}+row_id\\p{javaWhitespace}*=\\p{javaWhitespace}*(?<rowid>\\d+)";
}
