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
        byte[] record = buildRecord(colData,tabcolinfo);
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
    
    private byte[] buildRecord(ArrayList<Object> colData, TableColumnInfo tci){
        int totSize=0;
        for(int i=0;i<tci.numCols;++i){
            switch(tci.colDataTypes.get(i).getDataTypeAsInt()){
                case 9: int len = ((String)colData.get(i)).length();
                        if(len==0)
                            ++totSize;
                        else
                            totSize += len;
                        break;
                default: totSize += tci.colDataTypes.get(i).getDataTypeSize();break;
            }
        }
        byte[] record = new byte[totSize];
        
        //fill in record using ByteBuffer.putInt and all
        
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
