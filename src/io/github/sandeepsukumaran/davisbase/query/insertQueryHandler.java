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

import io.github.sandeepsukumaran.davisbase.exception.ArgumentCountMismatchException;
import io.github.sandeepsukumaran.davisbase.exception.BadInputValueException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidQuerySyntaxException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Sandeep
 */
public class insertQueryHandler {
    public insertQueryHandler(String inputQuery){
        query = inputQuery;
        insertAllPattern = Pattern.compile(INSERT_ALL_QUERY);
        insertPattern = Pattern.compile(INSERT_QUERY);
        insertAllMatcher = insertAllPattern.matcher(query);
        insertMatcher = insertPattern.matcher(query);
    }
    
    /**
     * Execute insert query.
     * @throws InvalidQuerySyntaxException
     * @throws io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException
     * @throws io.github.sandeepsukumaran.davisbase.exception.ArgumentCountMismatchException
     * @throws io.github.sandeepsukumaran.davisbase.exception.BadInputValueException
     */
    public void execute() throws InvalidQuerySyntaxException, NoSuchTableException, ArgumentCountMismatchException, BadInputValueException{
        if (insertAllMatcher.matches()){
            //select all query
            insertAllQueryExecute();
        }else if(insertMatcher.matches()){
            //select columns query
            insertQueryExecute();
        }else
            //should not happen. Must be caught in query parser itself.
            throw new InvalidQuerySyntaxException();
    }
    
    private void insertAllQueryExecute() throws NoSuchTableException, ArgumentCountMismatchException, BadInputValueException{
        ArrayList<String> tableNames = DavisBase.getTableNames();
        String tableName = insertAllMatcher.group("tablename");
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        TableColumnInfo tabcolinfo = DavisBase.getTableInfo(tableName);
        String values = insertAllMatcher.group("values");
        ArrayList<Object> colData = parseValues(tabcolinfo,values);
        
        //only TEXT fields maybe null indicated by ""
        ArrayList<Boolean>isnull = new ArrayList<>();
        for(int i=0;i<tabcolinfo.numCols;++i)
            if(tabcolinfo.colDataTypes.get(i).getDataTypeAsInt()!=9)
                isnull.add(false);
            else if(((String)colData.get(i)).equals(""))
                isnull.add(true);
        byte[] record = buildRecord(colData,tabcolinfo,isnull);
    }
    
    private void insertQueryExecute() throws NoSuchTableException{
        ArrayList<String> tableNames = DavisBase.getTableNames();
        String tableName = insertMatcher.group("tablename");
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        TableColumnInfo tabcolinfo = DavisBase.getTableInfo(tableName);
        String cols = insertAllMatcher.group("columns");
        String values = insertAllMatcher.group("values");
    }
    
    private ArrayList<Object> parseValues(TableColumnInfo tci, String values) throws ArgumentCountMismatchException, BadInputValueException{
        ArrayList<Object> colData = new ArrayList<>();
        values = values.substring(1,values.length());//ignore first and last ( and )
        //split on commas outside double quotation marks
        String[] tokens = values.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if(tokens.length != tci.numCols)
            throw new ArgumentCountMismatchException();
        else;
        SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat("YYYY-MM-DD_hh:mm:ss");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-DD");
        try{
            for(int col=0;col<tci.numCols;++col){
                switch(tci.colDataTypes.get(col).getDataTypeAsInt()){
                    case 1:
                        colData.add(Byte.parseByte(tokens[col]));
                        break;
                    case 2:
                        colData.add(Short.parseShort(tokens[col]));
                        break;
                    case 3:
                        colData.add(Integer.parseInt(tokens[col]));
                        break;
                    case 4:
                        colData.add(Long.parseLong(tokens[col]));
                        break;
                    case 5:
                        colData.add(Float.parseFloat(tokens[col]));
                        break;
                    case 6:
                        colData.add(Double.parseDouble(tokens[col]));
                        break;
                    case 7:
                        colData.add(simpleDateTimeFormat.parse(tokens[col]).getTime());
                        break;
                    case 8:
                        colData.add(simpleDateFormat.parse(tokens[col]).getTime());
                        break;
                    case 9:
                        colData.add(tokens[col]);
                        break;
                }
            }
        }catch(NumberFormatException|ParseException e){throw new BadInputValueException();}
        return colData;
    }
    
    private byte[] buildRecord(ArrayList<Object> colData, TableColumnInfo tci,ArrayList<Boolean> isnull){
        //fill in record using ByteBuffer.putInt and all
        ArrayList<Byte> recordData = new ArrayList<>();
        //header
        recordData.add((byte)0x00);recordData.add((byte)0x00);
        int rowid = (Integer)colData.get(0);
        ByteBuffer intbuffer = ByteBuffer.allocate(4);
        intbuffer.putInt(rowid);
        for(Byte b:intbuffer.array())
            recordData.add(b);
        
        //payload
        recordData.add((byte)tci.numCols);
        //write serial type codes
        for(int i=0;i<tci.numCols;++i)
            switch(tci.colDataTypes.get(i).getDataTypeAsInt()){
                case 1:
                    if(isnull.get(i))
                        recordData.add((byte)0x00);
                    else
                        recordData.add((byte)0x04);
                    break;
                case 2:
                    if(isnull.get(i))
                        recordData.add((byte)0x01);
                    else
                        recordData.add((byte)0x05);
                    break;
                case 3:
                    if(isnull.get(i))
                        recordData.add((byte)0x02);
                    else
                        recordData.add((byte)0x06);
                    break;
                case 4:
                    if(isnull.get(i))
                        recordData.add((byte)0x03);
                    else
                        recordData.add((byte)0x07);
                    break;
                case 5:
                    if(isnull.get(i))
                        recordData.add((byte)0x02);
                    else
                        recordData.add((byte)0x08);
                    break;
                case 6:
                    if(isnull.get(i))
                        recordData.add((byte)0x03);
                    else
                        recordData.add((byte)0x09);
                    break;
                case 7:
                    if(isnull.get(i))
                        recordData.add((byte)0x03);
                    else
                        recordData.add((byte)0x0a);
                    break;
                case 8:
                    if(isnull.get(i))
                        recordData.add((byte)0x03);
                    else
                        recordData.add((byte)0x0b);
                    break;
                case 9:
                    if(isnull.get(i))
                        recordData.add((byte)0x0c);
                    else
                        recordData.add((byte)(0x0c + ((String)colData.get(i)).length()));
                    break;
            }
        
        //write binary column data
        for(int i=0;i<tci.numCols;++i){
            switch(tci.colDataTypes.get(i).getDataTypeAsInt()){
                case 1:
                    if(isnull.get(i))
                        recordData.add((byte)0x00);
                    else
                        recordData.add((Byte)colData.get(i));
                    break;
                case 2:
                    if(isnull.get(i)){
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                    }else{
                        short sdata = (Short)colData.get(i);
                        ByteBuffer shortbuffer = ByteBuffer.allocate(2);
                        shortbuffer.putShort(sdata);
                        for(Byte b:shortbuffer.array())
                            recordData.add(b);
                    }
                    break;
                case 3:
                    if(isnull.get(i)){
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                    }else{
                        int idata = (Integer)colData.get(i);
                        intbuffer = ByteBuffer.allocate(4);
                        intbuffer.putInt(idata);
                        for(Byte b:intbuffer.array())
                            recordData.add(b);
                    }
                    break;
                case 4:
                case 7:
                case 8:
                    if(isnull.get(i)){
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                    }else{
                        long ldata = (Long)colData.get(i);
                        ByteBuffer longbuffer = ByteBuffer.allocate(8);
                        longbuffer.putLong(ldata);
                        for(Byte b:longbuffer.array())
                            recordData.add(b);
                    }
                    break;
                case 5:
                    if(isnull.get(i)){
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                    }else{
                        float fdata = (Float)colData.get(i);
                        ByteBuffer floatbuffer = ByteBuffer.allocate(4);
                        floatbuffer.putFloat(fdata);
                        for(Byte b:floatbuffer.array())
                            recordData.add(b);
                    }
                    break;
                case 6:
                    if(isnull.get(i)){
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                        recordData.add((byte)0x00);recordData.add((byte)0x00);
                    }else{
                        double ddata = (Double)colData.get(i);
                        ByteBuffer doublebuffer = ByteBuffer.allocate(8);
                        doublebuffer.putDouble(ddata);
                        for(Byte b:doublebuffer.array())
                            recordData.add(b);
                    }
                    break;
                case 9:
                    if(isnull.get(i))
                        recordData.add((byte)0x00);
                    else
                        for(Byte b:((String)colData.get(i)).getBytes())
                            recordData.add(b);
            }
        }
        short payloadsize = (short)(recordData.size()-6);
        ByteBuffer shortbuffer = ByteBuffer.allocate(2);
        shortbuffer.putShort(payloadsize);
        byte[] plsize = shortbuffer.array();
        recordData.set(0,plsize[0]);
        recordData.set(1,plsize[1]);
        
        byte[] record = new byte[recordData.size()];
        for(int i=0;i<recordData.size();++i)
            record[i] = recordData.get(i);
        return record;
    }
    
    private final Pattern insertAllPattern;
    private final Pattern insertPattern;
    private final Matcher insertAllMatcher;
    private final Matcher insertMatcher;
    private final String query;
    private final String INSERT_ALL_QUERY = "^insert into (?<tablename>\\w+)\\p{javaWhitespace}*values\\p{javaWhitespace}*(?<values>\\((\\d+(\\.\\d+)?)|(\"(\\p{Punct}&&[^\"\'])+\")(\\p{javaWhitespace}*,\\p{javaWhitespace}*(\\d+(\\.\\d+)?)|\"(\\p{Punct}&&[^\"\'])+\")*\\));$";
    private final String INSERT_QUERY = "^insert into (?<tablename>\\w+)\\p{javaWhitespace}*(?<colnames>\\(\\w+(\\p{javaWhitespace}*,\\p{javaWhitespace}*\\w+)*\\))\\p{javaWhitespace}*values\\p{javaWhitespace}*(?<values>\\((\\d+(\\.\\d+)?)|(\"(\\p{Punct}&&[^\"\'])+\")(\\p{javaWhitespace}*,\\p{javaWhitespace}*(\\d+(\\.\\d+)?)|\"(\\p{Punct}&&[^\"\'])+\")*\\));$";
}
