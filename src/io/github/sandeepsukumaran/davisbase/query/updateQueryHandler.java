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
import static io.github.sandeepsukumaran.davisbase.datatype.DataType.SIMPLEDATEFORMAT;
import static io.github.sandeepsukumaran.davisbase.datatype.DataType.SIMPLEDATETIMEFORMAT;
import io.github.sandeepsukumaran.davisbase.exception.BadInputValueException;
import io.github.sandeepsukumaran.davisbase.exception.BadWhereClauseValueException;
import io.github.sandeepsukumaran.davisbase.exception.ColumnCannotBeNullException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoDirectMetaDataModificationException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import io.github.sandeepsukumaran.davisbase.helpermethods.HelperMethods;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import io.github.sandeepsukumaran.davisbase.tree.ReadRows;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Sandeep
 */
public class updateQueryHandler {
    public updateQueryHandler(String inputQuery){
        query = inputQuery;
        updatePattern = Pattern.compile(UPDATE_QUERY);
        updateMatcher = updatePattern.matcher(query);
        updateMatcher.matches();
    }
    
    /**
     *
     * @throws NoSuchTableException
     * @throws MissingTableFileException
     * @throws IOException
     * @throws FileNotFoundException
     * @throws InvalidTableInformationException
     * @throws NoSuchColumnException
     * @throws io.github.sandeepsukumaran.davisbase.exception.BadWhereClauseValueException
     * @throws io.github.sandeepsukumaran.davisbase.exception.BadInputValueException
     * @throws io.github.sandeepsukumaran.davisbase.exception.NoDirectMetaDataModificationException
     * @throws io.github.sandeepsukumaran.davisbase.exception.ColumnCannotBeNullException
     */
    public void execute() throws NoSuchTableException, MissingTableFileException, IOException, FileNotFoundException, InvalidTableInformationException, NoSuchColumnException, BadWhereClauseValueException, BadInputValueException, NoDirectMetaDataModificationException, ColumnCannotBeNullException{
        /*
        Idea: Use new ArrayList<Byte> to create the new record and overwrite if
        condition satisfied.
        */
        String tableName = updateMatcher.group("tablename");
        String tarCol = updateMatcher.group("tarcol");
        String tarVal = updateMatcher.group("tarval");
        int tarColIndex = validateTableandColNames(tableName, tarCol, tarVal);
        TableColumnInfo tci= DavisBase.getTableInfo(tableName);
        Object tarval = null;
        DataType tarColDType = tci.colDataTypes.get(tarColIndex);
        if(!tarVal.equals("null"))
                tarval = DataType.valueToObject(tarVal, tarColDType);
        else{}
        String whereClause = updateMatcher.group("whereclause");
        boolean whereExists = (whereClause!=null);
        String wherecol; String whereval=null; int wherecolindex = -1;
        if(whereExists){
            wherecol = updateMatcher.group("wherecol");
            whereval = updateMatcher.group("whereval");
            wherecolindex = validateWhereClause(tableName, tci,wherecol,whereval);
        }else{}
        performUpdate(tableName, tci,tarColIndex, tarval, tarColDType, wherecolindex, whereval, whereClause);
    }
    
    private void performUpdate(String tableName, TableColumnInfo tci, int tarColIndex, Object tarVal, DataType tarColDType, int wherecolindex, String whereVal, String whereclause) throws NoDirectMetaDataModificationException, MissingTableFileException, FileNotFoundException, IOException, InvalidTableInformationException{
        //wherecolindex = -1 => no where clause
        boolean whereexists = (wherecolindex!=-1);
        DataType whereDType = null;
        String op = null;
        if(whereexists){
            whereDType = tci.colDataTypes.get(wherecolindex);
            whereclause = whereclause.trim().substring(5).trim();
            int pos = whereclause.indexOf('=');
            if (pos != -1)
                //comparison operator used in query
                if ((whereclause.charAt(pos-1)!='>') && (whereclause.charAt(pos-1)!='<'))
                    op = "=";
                else
                    op = whereclause.substring(pos-1,pos+1);//pos is location of =
            else if(whereclause.contains("<>"))
                op = "<>";
            else if(whereclause.contains("<"))
                op = "<";
            else if(whereclause.contains(">"))
                op = ">";
            else if(whereclause.endsWith("is null"))
                op = "is null";
            else if(whereclause.endsWith("is not null"))
                op = "is not null";
            else{}
        }else{}
        String workingDirectory = System.getProperty("user.dir"); // gets current working directory
            if(tableName.equals("davisbase_tables")||tableName.equals("davisbase_columns"))
                throw new NoDirectMetaDataModificationException();
            else{}
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
                    
                    boolean pass = true;
                    
                    short payloadSize = tableFile.readShort();//read payload size
                    int rowid = tableFile.readInt();//read row_id
                    tableFile.skipBytes(1);//skip over number of columns
                    ArrayList<Byte> serialCodes = new ArrayList<>(); 
                    for(int col=1;col<tci.numCols;++col)
                        serialCodes.add(tableFile.readByte());
                    long pos = tableFile.getFilePointer();// start of binary data
                    if(whereexists){
                        if(wherecolindex!=0){//not based on primary key
                            int numbytesSkipOver = HelperMethods.byteSumUpto(serialCodes,(byte)(wherecolindex-1)); //-1 accounts for row_id not being in list
                            tableFile.skipBytes(numbytesSkipOver);
                            pass = readAndEvaluate(tableFile, serialCodes.get(wherecolindex-1), whereDType, whereVal, op);
                        }else{//based on primary key
                            switch(op){
                                case "is null":pass = false;break;
                                case "is not null": pass = true;break;
                                default:
                                    pass = ReadRows.evaluate("int", (Integer)rowid, Integer.parseInt(whereVal), op);
                            }
                        }
                    }else{}//pass by default since no where clause
                    
                    if(!pass)
                        continue;
                    else{}
                    //perform actual update
                    tableFile.seek(pos);//go to start of binary data
                    //skip upto required column
                    tableFile.skipBytes(HelperMethods.byteSumUpto(serialCodes,(byte)(tarColIndex-1))); //-1 accounts for row_id not being in list
                    pos = tableFile.getFilePointer();//position of required column
                    //skip over required column
                    tableFile.skipBytes(DataType.getDataTypeSize(serialCodes.get(tarColIndex-1)));
                    //read remaining data in record
                    byte[] remainingData = new byte[HelperMethods.byteSumAfter(serialCodes,(byte)(tarColIndex-1))];
                    tableFile.read(remainingData);
                    tableFile.seek(pos);//go back to required column
                    
                    //write out new value and remaining data
                    writeUpdatedField(tableFile, tarColDType, tarVal, remainingData);
                    
                    //update payload size in record header
                    tableFile.seek(pageStart+cellLocation);
                    if(!tarColDType.toString().equals("text"))
                        tableFile.skipBytes(2);//writeShort(payloadSize);
                    else
                        //replacement of null strings is not permitted
                        tableFile.writeShort((short)(payloadSize - (serialCodes.get(tarColIndex-1)-0x0c)+(((String)tarVal).length())));
                    
                    tableFile.skipBytes(5);//row_id and number of columns
                    tableFile.skipBytes(tarColIndex-1);//-1 to account for absence of row_id
                    
                    //update serial code
                    writeUpdatedSerialCode(tableFile, tarColDType, tarVal);
                }
                curPage = nextPage;
            }
            tableFile.close();
    }
    
    private boolean readAndEvaluate(RandomAccessFile fp, Byte serialCode, DataType dtype, String whereVal, String op) throws IOException, NumberFormatException{
        switch(op){
            case "is null":
                return DataType.isNullSerialCode(serialCode);
            case "is not null":
                return !DataType.isNullSerialCode(serialCode);
        }
        //op is one of the comparator operators
        switch(dtype.getDataTypeAsInt()){
            case 1:
                return ReadRows.evaluate(dtype.toString(),fp.readByte(),Byte.parseByte(whereVal),op);
            case 2:
                return ReadRows.evaluate(dtype.toString(),fp.readShort(),Short.parseShort(whereVal),op);
            case 3:
                return ReadRows.evaluate(dtype.toString(),fp.readInt(),Integer.parseInt(whereVal),op);
            case 4:
                return ReadRows.evaluate(dtype.toString(),fp.readLong(),Long.parseLong(whereVal),op);
            case 5:
                return ReadRows.evaluate(dtype.toString(),fp.readFloat(),Float.parseFloat(whereVal),op);
            case 6:
                return ReadRows.evaluate(dtype.toString(),fp.readDouble(),Double.parseDouble(whereVal),op);
            case 7:
                Date dt = SIMPLEDATETIMEFORMAT.parse(whereVal,new ParsePosition(0));
                return ReadRows.evaluate(dtype.toString(),fp.readLong(),(Long)dt.getTime(),op);
            case 8:
                Date d = SIMPLEDATEFORMAT.parse(whereVal,new ParsePosition(0));
                return ReadRows.evaluate(dtype.toString(),fp.readLong(),(Long)d.getTime(),op);
            case 9:
                byte[] readstring = new byte[serialCode-0x0c];
                fp.read(readstring);
                return ReadRows.evaluate(dtype.toString(),new String(readstring),whereVal.substring(1,whereVal.length()-1),op);
        }
        return false;
    }
    
    private void writeUpdatedField(RandomAccessFile fp, DataType dtype, Object tarVal, byte[] remainingData) throws IOException{
        if(tarVal==null){
            //write out suitable null value
            //writing out of non-string value does not affect remaining bytes
            switch(dtype.getDataTypeAsInt()){
                case 1: fp.writeByte(0x00);
                        return;
                case 2: fp.writeByte(0x00);fp.writeByte(0x00);
                        return;
                case 3:
                case 5: fp.writeByte(0x00);fp.writeByte(0x00);
                        fp.writeByte(0x00);fp.writeByte(0x00);
                        return;
                case 4:
                case 6:
                case 7:
                case 8: fp.writeByte(0x00);fp.writeByte(0x00);
                        fp.writeByte(0x00);fp.writeByte(0x00);
                        fp.writeByte(0x00);fp.writeByte(0x00);
                        fp.writeByte(0x00);fp.writeByte(0x00);
                        return;
                case 9: fp.writeByte(0x00);fp.write(remainingData);return;
            }
        }else{
            //write out actual value
            switch(dtype.getDataTypeAsInt()){
                case 1: fp.writeByte((Byte)tarVal); return;
                case 2: fp.writeShort((Short)tarVal); return;
                case 3: fp.writeInt((Integer)tarVal); return;
                case 4:
                case 7:
                case 8: fp.writeLong((Long)tarVal); return;
                case 5: fp.writeFloat((Float)tarVal); return;
                case 6: fp.writeDouble((Double)tarVal); return;
                case 9: fp.write(((String)tarVal).getBytes());fp.write(remainingData);return;
            }
        }
    }
    
    private void writeUpdatedSerialCode(RandomAccessFile fp, DataType dtype, Object tarVal) throws IOException{
        if(tarVal==null){
            //write out suitable null serial code
            switch(dtype.getDataTypeAsInt()){
                case 1: fp.writeByte(0x00);return;
                case 2: fp.writeByte(0x01);return;
                case 3:
                case 5: fp.writeByte(0x02);return;
                case 4:
                case 6:
                case 7:
                case 8: fp.writeByte(0x03);return;
                case 9: fp.writeByte(0x0c);return;
            }
        }else{
            //write out actual serial code
            switch(dtype.getDataTypeAsInt()){
                case 1: fp.writeByte(0x04); return;
                case 2: fp.writeByte(0x05); return;
                case 3: fp.writeByte(0x06); return;
                case 4: fp.writeByte(0x07); return;
                case 5: fp.writeByte(0x08); return;
                case 6: fp.writeByte(0x09); return;
                case 7: fp.writeByte(0x0a); return;
                case 8: fp.writeByte(0x0b); return;
                case 9: fp.writeByte(0x0c+((String)tarVal).length());return;
            }
        }
    }
    private int validateTableandColNames(String tableName, String colName, String tarVal) throws NoSuchTableException, MissingTableFileException, IOException, FileNotFoundException, InvalidTableInformationException, NoSuchColumnException, BadInputValueException, ColumnCannotBeNullException{
        int index;
        ArrayList<String> tableNames = DavisBase.getTableNames();
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        ArrayList<String> tablecols = DavisBase.getTableColumns(tableName);
        TableColumnInfo tci = DavisBase.getTableInfo(tableName);
        if (!tablecols.contains(colName))
            throw new NoSuchColumnException(colName,tableName);
        else
            index = tablecols.indexOf(colName);
        if(tarVal.equals("null"))
            if(tci.colNullable.get(index))
                return index;
            else
                throw new ColumnCannotBeNullException(colName);
        else{}
        if(!DataType.validData(tarVal,DavisBase.getTableInfo(tableName).colDataTypes.get(index)))
                throw new BadInputValueException();
        return index;
    }
    
    private int validateWhereClause(String tableName, TableColumnInfo tci, String wherecol, String whereval) throws MissingTableFileException, NoSuchColumnException, InvalidTableInformationException, IOException, BadWhereClauseValueException{
        int index;
        ArrayList<String> tablecols = tci.colNames;
        if (!tablecols.contains(wherecol))
            throw new NoSuchColumnException(wherecol,tableName);
        else
            index = tablecols.indexOf(wherecol);
        if(whereval!=null){
            if(!DataType.validData(whereval,tci.colDataTypes.get(tablecols.indexOf(wherecol))))
                throw new BadWhereClauseValueException(wherecol,whereval);
            else{}
        }else{}//is null or is not null
        return index;
    }
    
    private final String query;
    private final Pattern updatePattern;
    private final Matcher updateMatcher;
    private final String UPDATE_QUERY = "update (?<tablename>\\w+) set (?<tarcol>\\w+)\\p{javaWhitespace}*=\\p{javaWhitespace}*(?<tarval>(-?\\d+(\\.\\d+)?)|(\"([\\p{Graph}&&[^\"\']])+\")|(null)|(\\d{4}-\\d{2}-\\d{2}_\\d{2}:\\d{2}:\\d{2})|(\\d{4}-\\d{2}-\\d{2}))\\p{javaWhitespace}*(?<whereclause> where (?<wherecol>\\w+)((\\p{javaWhitespace}*(=|<=|<|>|>=|<>)\\p{javaWhitespace}*(?<whereval>(-?\\d+(\\.\\d+)?)|(\"([\\p{Graph}&&[^\"\']])+\")|(\\d{4}-\\d{2}-\\d{2}_\\d{2}:\\d{2}:\\d{2})|(\\d{4}-\\d{2}-\\d{2})))|(\\p{javaWhitespace}+is null)|((\\p{javaWhitespace}+is not null))))?";
}
