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
package io.github.sandeepsukumaran.davisbase.query;

import io.github.sandeepsukumaran.davisbase.datatype.DataType;
import io.github.sandeepsukumaran.davisbase.exception.InvalidDataTypeName;
import io.github.sandeepsukumaran.davisbase.exception.InvalidPKException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoPKException;
import io.github.sandeepsukumaran.davisbase.exception.TableAlreadyExistsException;
import io.github.sandeepsukumaran.davisbase.helpermethods.HelperMethods;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import io.github.sandeepsukumaran.davisbase.tree.UpdateRecord;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Sandeep
 */
public class createQueryHandler {
    public createQueryHandler(String inputQuery){
    query = inputQuery;
    createPattern = Pattern.compile(CREATE_QUERY);
    createMatcher = createPattern.matcher(query);
    createMatcher.matches();
    attrLinePattern = Pattern.compile(ATTR_LINE);
    }
    
    static{
        TABLEMETADATANUMCOLS = 4;
        TABLEMETADATACOLNAMES = new String[]{"row_id","table_name","record_count","root_page"};
        TABLEMETADATACOLDATATYPES = new DataType[]{new DataType("int"),new DataType("text"),new DataType("int"),new DataType("smallint")};
        TABLEMETADATACOLNULLABLE = new Boolean[]{false,false,false,false};
        
        TABLECOLMETADATANUMCOLS = 6;
        TABLECOLMETADATACOLNAMES = new String[]{"row_id","table_name","column_name","data_type","ordinal_position","is_nullable"};
        TABLECOLMETADATACOLDATATYPES = new DataType[]{new DataType("int"),new DataType("text"),new DataType("text"),new DataType("text"),new DataType("tinyint"),new DataType("text")};
        TABLECOLMETADATACOLNULLABLE = new Boolean[]{false,false,false,false,false,false};
    }
    
    /**
     *
     * @throws TableAlreadyExistsException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidDataTypeName
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidPKException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     * @throws io.github.sandeepsukumaran.davisbase.exception.NoPKException
     */
    public void execute() throws TableAlreadyExistsException, InvalidDataTypeName, InvalidPKException, FileNotFoundException, IOException, MissingTableFileException, InvalidTableInformationException, NoPKException{
        ArrayList<String> tableNames = DavisBase.getTableNames();
        String tableName = createMatcher.group("tablename");
        if (tableNames.contains(tableName))
            throw new TableAlreadyExistsException(tableName);
        else;
        String attributes = createMatcher.group("attrlist");
        attributes = attributes.trim();//substring(1,attributes.length()-1);//remove first and last ( and )
        //split into individual attribute declarations based on commas outside quotation marks
        String[] attributeLines = attributes.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        
        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<DataType> colDataType = new ArrayList<>();
        ArrayList<Boolean> nullability = new ArrayList<>();
        
        Matcher attrLineMatcher;
        for(String attrline:attributeLines){
            attrline = attrline.trim();
            attrLineMatcher = attrLinePattern.matcher(attrline);
            attrLineMatcher.matches();
            String attrname = attrLineMatcher.group("attrname");
            String dtype = attrLineMatcher.group("datatype");
            String constraints = attrLineMatcher.group("constraints");//may be null
            switch(dtype){
                case "tinyint":
                case "smallint":
                case "int":    
                case "bigint":
                case "real":
                case "double":
                case "datetime":
                case "date":
                case "text":
                    colDataType.add(new DataType(dtype));
                    break;
                default:
                    throw new InvalidDataTypeName(dtype);
                        
            }
            colNames.add(attrname);
            if(constraints!=null){
                constraints = constraints.trim();
                if(constraints.charAt(0)=='p'){//primary key
                    if(!attrname.equals("row_id"))
                        throw new InvalidPKException();
                    else if(!dtype.equals("int"))
                        throw new InvalidPKException();
                    else
                        nullability.add(false);
                }else//not null
                    nullability.add(false);
            }else
                nullability.add(true);
        }
        
        //rearranging so that row_id PRIMARY KEY is always first. Violates ordinal_position but no time to fix it.
        int pkindex = colNames.indexOf("row_id");
        if(pkindex==-1)
            throw new NoPKException();
        else{}
        colNames.remove(pkindex);
        colNames.add(0,"row_id");
        colDataType.remove(pkindex);
        colDataType.add(0,new DataType("int"));
        nullability.remove(pkindex);
        nullability.add(0,false);
        
        //write data to files
        //add meta data to davisbase_tables.tbl
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
        String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_tables.tbl";
        File file = new File(absoluteFilePath);
        if (!(file.exists() && !file.isDirectory())){
            throw new MissingTableFileException("davisbase_tables");
        }else;

        RandomAccessFile dbtablesFile = new RandomAccessFile(absoluteFilePath, "rw");
        updateTablesMetadataFile(tableName,dbtablesFile);
        dbtablesFile.close();
        
        //add meta data to davisbase_columns.tbl
        workingDirectory = System.getProperty("user.dir"); // gets current working directory
        absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_columns.tbl";
        File colfile = new File(absoluteFilePath);
        if (!(colfile.exists() && !colfile.isDirectory())){
            throw new MissingTableFileException("davisbase_columns");
        }else;

        RandomAccessFile dbcolsFile = new RandomAccessFile(absoluteFilePath, "rw");
        updateTableColumnsMetadataFile(tableName,colNames,colDataType,nullability,dbcolsFile);
        dbcolsFile.close();
        //write table's new file
        absoluteFilePath = workingDirectory + File.separator + "data" + File.separator + "user_data" + File.separator + tableName+".tbl";

        RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");
        tableFile.setLength(DavisBase.PAGESIZE);
        tableFile.seek(0);
        tableFile.writeByte(0x0d);//type of page - leaf
        tableFile.writeByte(0x00);//no records in file
        tableFile.writeShort((short)DavisBase.PAGESIZE);//start of cell area is at the very end of file
        tableFile.writeInt(-1);//no sibling
        tableFile.close();
        
        UpdateRecord.incrementRecordCount("davisbase_tables");
        DavisBase.populateTableNames();
    }
    
    private void updateTablesMetadataFile(String tableName, RandomAccessFile file) throws IOException, InvalidTableInformationException, MissingTableFileException{
        if(file.length()<DavisBase.PAGESIZE)
            throw new InvalidTableInformationException("davisbase_tables");
        else{}
        int numPages = (int)(file.length()/DavisBase.PAGESIZE);
        int curPage;
        if(numPages==3)
            curPage=2;
        else
            curPage=numPages;
        
        file.seek((curPage-1)*DavisBase.PAGESIZE);
        file.skipBytes(1);
        int numCells = file.readByte();
        file.skipBytes(2);
        file.skipBytes(4);
        file.skipBytes(2*(numCells-1));
        int highestcellstart = file.readShort();
        file.seek((curPage-1)*DavisBase.PAGESIZE + highestcellstart);
        file.skipBytes(2);
        int highestrow_id = file.readInt();
        
        ArrayList<Object> insertionData = new ArrayList<>();
        insertionData.add((Integer)(highestrow_id+1));
        insertionData.add(tableName);
        insertionData.add((Integer)(0));
        insertionData.add((Short)((short)1));
        
        TableColumnInfo tci = new TableColumnInfo();
        tci.numCols = TABLEMETADATANUMCOLS;
        tci.colNames = new ArrayList<>(Arrays.asList(TABLEMETADATACOLNAMES));
        tci.colDataTypes = new ArrayList<>(Arrays.asList(TABLEMETADATACOLDATATYPES));
        tci.colNullable = new ArrayList<>(Arrays.asList(TABLEMETADATACOLNULLABLE));
        
        byte[] record = insertQueryHandler.buildRecord(insertionData, tci, tci.colNullable);
        
        if(curPage==1)
            if(HelperMethods.writeRecordToFirstPage(file, record, highestrow_id+1))
                UpdateRecord.setRootPage("davisbase_tables");
            else{}
        else
            HelperMethods.writeRecordToPage(file, record, curPage, highestrow_id+1);
    }
    
    private void updateTableColumnsMetadataFile(String tableName,ArrayList<String> colNames, ArrayList<DataType> colDataType, ArrayList<Boolean> nullability, RandomAccessFile file) throws IOException, InvalidTableInformationException, MissingTableFileException{
        if(file.length()<DavisBase.PAGESIZE)
            throw new InvalidTableInformationException("davisbase_columns");
        else{}
        int numPages = (int)(file.length()/DavisBase.PAGESIZE);
        int curPage;
        if(numPages==3)
            curPage=2;
        else
            curPage=numPages;
        
        file.seek((curPage-1)*DavisBase.PAGESIZE);
        file.skipBytes(1);
        int numCells = file.readByte();
        file.skipBytes(2);
        file.skipBytes(4);
        file.skipBytes(2*(numCells-1));
        int highestcellstart = file.readShort();
        file.seek((curPage-1)*DavisBase.PAGESIZE + highestcellstart);
        file.skipBytes(2);
        int highestrow_id = file.readInt();
        
        TableColumnInfo tci = new TableColumnInfo();
        tci.numCols = TABLECOLMETADATANUMCOLS;
        tci.colNames = new ArrayList<>(Arrays.asList(TABLECOLMETADATACOLNAMES));
        tci.colDataTypes = new ArrayList<>(Arrays.asList(TABLECOLMETADATACOLDATATYPES));
        tci.colNullable = new ArrayList<>(Arrays.asList(TABLECOLMETADATACOLNULLABLE));
        
        for(int i=0;i<colNames.size();++i){
            ArrayList<Object> insertionData = new ArrayList<>();
            insertionData.add((Integer)(highestrow_id+1+i));
            insertionData.add(tableName);
            insertionData.add(colNames.get(i));
            insertionData.add(colDataType.get(i).toString());
            insertionData.add((byte)(i+1));
            if(nullability.get(i))
                insertionData.add("yes");
            else
                insertionData.add("no");
            
            byte[] record = insertQueryHandler.buildRecord(insertionData, tci, tci.colNullable);
            //System.out.println(Arrays.toString(record));
            if(curPage==1)
                if(HelperMethods.writeRecordToFirstPage(file, record, highestrow_id+1+i))
                    UpdateRecord.setRootPage("davisbase_columns");
                else{}
            else
                HelperMethods.writeRecordToPage(file, record, curPage, highestrow_id+1+i);
            
            UpdateRecord.incrementRecordCount("davisbase_columns");
            numPages = (int)(file.length()/DavisBase.PAGESIZE);
            if(numPages==3)
                curPage=2;
            else
                curPage=numPages;
        }
    }
    
    private final String query;
    private final Pattern createPattern;
    private final Matcher createMatcher;
    private final Pattern attrLinePattern;
    private final String ATTR_LINE = "^(?<attrname>\\p{Alpha}\\w*)\\p{javaWhitespace}+(?<datatype>\\p{Alpha}+)\\p{javaWhitespace}*(?<constraints>\\p{javaWhitespace}+((primary\\p{javaWhitespace}+key)|(not\\p{javaWhitespace}+null)))?\\p{javaWhitespace}*$";
    private final String CREATE_QUERY = "^create\\p{javaWhitespace}+table\\p{javaWhitespace}+(?<tablename>\\p{Alpha}\\w*)\\p{javaWhitespace}*\\((?<attrlist>(\\p{javaWhitespace}*\\p{Alpha}\\w*\\p{javaWhitespace}+\\p{Alpha}+\\p{javaWhitespace}*(\\p{javaWhitespace}+((primary\\p{javaWhitespace}+key)|(not\\p{javaWhitespace}+null)))?\\p{javaWhitespace}*,)*(\\p{javaWhitespace}*\\p{Alpha}\\w*\\p{javaWhitespace}+\\p{Alpha}+\\p{javaWhitespace}*(\\p{javaWhitespace}+((primary\\p{javaWhitespace}+key)|(not\\p{javaWhitespace}+null)))?\\p{javaWhitespace}*))\\)$";
    
    public static final Boolean[] TABLEMETADATACOLNULLABLE;
    public static final DataType[] TABLEMETADATACOLDATATYPES;
    public static final int TABLEMETADATANUMCOLS;
    public static final String[] TABLEMETADATACOLNAMES;
    
    public static final Boolean[] TABLECOLMETADATACOLNULLABLE;
    public static final DataType[] TABLECOLMETADATACOLDATATYPES;
    public static final int TABLECOLMETADATANUMCOLS;
    public static final String[] TABLECOLMETADATACOLNAMES;
}
//each statement will have attr_name data_type (two words)? [PRIMARY KEY|NOT NULL]