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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author sandeep
 */
public class dropQueryHandler {
    public dropQueryHandler(String inputQuery){
        query = inputQuery;
        dropPattern = Pattern.compile(DROP_QUERY);
        dropMatcher = dropPattern.matcher(query);
        dropMatcher.matches();
    }
    //baseic idea: in davisbase_columns read entry, if table name matches, delete that entry from list of pointers.
    // easy since we read list of cell locations. do same for davisbase_tables entry
    //AND decrement row count

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
        String tableName = dropMatcher.group("tablename");
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        deleteFromTablesMetaDataFile(tableName);
        deleteFromTableColumnsMetaDataFile(tableName);
        DavisBase.populateTableNames();
        
        String workingDirectory = System.getProperty("user.dir");
        Path dataFolderPath = Paths.get(workingDirectory+FileSystems.getDefault().getSeparator()+"data");
        try{
            Files.delete(Paths.get(dataFolderPath.toString()+FileSystems.getDefault().getSeparator()+"user_data"+FileSystems.getDefault().getSeparator()+tableName+".tbl"));
        }catch(IOException e){//File missing from file structure but that's okay   
        }
    }
    
    private void deleteFromTablesMetaDataFile(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        //affect single entry
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
        String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_tables.tbl";
        File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            throw new MissingTableFileException("davisbase_tables");
        }else;
        
        RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
        if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
            throw new InvalidTableInformationException("davisbase_tables");
        else;
        
        int curPage=1;
        while(curPage!=-1){
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
                tableFile.skipBytes(7);//skip over header+numColumns
                byte nameLen = (byte)(tableFile.readByte()-0x0c);
                tableFile.skipBytes(2);//skip over length of record_count and root_page
                byte[] b = new byte[nameLen];
                tableFile.read(b);
                String tblName = new String(b);
                if(!tblName.equals(tableName))
                    continue;
                else{}
                page_dirty = true;
                cellLocations.remove(i);
                --i;
            }
            if(page_dirty){
                tableFile.seek(pageStart+1);
                tableFile.writeByte((byte)cellLocations.size());
                tableFile.skipBytes(6);
                for(Short cellLocation:cellLocations)
                    tableFile.writeShort(cellLocation);
            }else{}
            curPage = nextPage;
        }
        UpdateRecord.decrementRecordCount("davisbase_tables");
    }
    
    private void deleteFromTableColumnsMetaDataFile(String tableName) throws MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        //reduce row count by as many columns in table
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
        String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_columns.tbl";
        File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            throw new MissingTableFileException("davisbase_columns");
        }else;
        
        RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
        if(tableFile.length() < DavisBase.PAGESIZE) //no meta data information found
            throw new InvalidTableInformationException("davisbase_columns");
        else;
        
        int curPage=1;int numCols=0;
        while(curPage!=-1){
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
                tableFile.skipBytes(7);//skip over header+numColumns
                byte nameLen = (byte)(tableFile.readByte()-0x0c);
                tableFile.skipBytes(4);//skip over lengths of column_name, data_type, ordinal_position,and is_nullable
                byte[] b = new byte[nameLen];
                tableFile.read(b);
                String tblName = new String(b);
                if(!tblName.equals(tableName))
                    continue;
                else{}
                ++numCols;
                page_dirty = true;
                cellLocations.remove(i);
                --i;
            }
            if(page_dirty){
                tableFile.seek(pageStart+1);
                tableFile.writeByte((byte)cellLocations.size());
                tableFile.skipBytes(6);
                for(Short cellLocation:cellLocations)
                    tableFile.writeShort(cellLocation);
            }else{}
            curPage = nextPage;
        }
        
        UpdateRecord.decrementRecordCount("davisbase_columns",numCols);
    }
    private final Pattern dropPattern;
    private final Matcher dropMatcher;
    private final String DROP_QUERY = "drop\\p{javaWhitespace}+table\\p{javaWhitespace}+(?<tablename>\\p{Alpha}\\w*)";
    private final String query;
}
