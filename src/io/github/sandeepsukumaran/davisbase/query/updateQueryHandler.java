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
import io.github.sandeepsukumaran.davisbase.exception.BadInputValueException;
import io.github.sandeepsukumaran.davisbase.exception.BadWhereClauseValueException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableColumnInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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
     */
    public void execute() throws NoSuchTableException, MissingTableFileException, IOException, FileNotFoundException, InvalidTableInformationException, NoSuchColumnException, BadWhereClauseValueException, BadInputValueException{
        /*
        Idea: Use new ArrayList<Byte> to create the new record and overwrite if
        condition satisfied.
        */
        String tableName = updateMatcher.group("tablename");
        String tarCol = updateMatcher.group("tarcol");
        String tarVal = updateMatcher.group("tarval");
        int tarColIndex = validateTableandColNames(tableName, tarCol, tarVal);
        TableColumnInfo tci= DavisBase.getTableInfo(tableName);
        String whereClause = updateMatcher.group("whereclause");
        boolean whereExists = (whereClause!=null);
        String wherecol; String whereval=null; int wherecolindex = -1;
        if(whereExists){
            wherecol = updateMatcher.group("whereclause");
            whereval = updateMatcher.group("whereval");
            wherecolindex = validateWhereClause(tableName, tci,wherecol,whereval);
        }else{}
        performUpdate(tci,tarColIndex, tarVal, wherecolindex, whereval, whereClause);
    }
    
    private void performUpdate(TableColumnInfo tci, int tarColIndex, String tarVal, int wherecolindex, String whereVal, String whereclause){
        //wherecolindex = -1 => no where clause
    }
    
    private int validateTableandColNames(String tableName, String colName, String tarVal) throws NoSuchTableException, MissingTableFileException, IOException, FileNotFoundException, InvalidTableInformationException, NoSuchColumnException, BadInputValueException{
        int index;
        ArrayList<String> tableNames = DavisBase.getTableNames();
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        ArrayList<String> tablecols = DavisBase.getTableColumns(tableName);
        if (!tablecols.contains(colName))
            throw new NoSuchColumnException(colName,tableName);
        else
            index = tablecols.indexOf(colName);
        if(tarVal.equals("null"))
            return index;
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
    private final String UPDATE_QUERY = "update (?<tablename>\\w+) set (?<tarcol>\\w+)\\p{javaWhitespace}*=\\p{javaWhitespace}*(?<tarval>(\\d+(\\.\\d+)?)|(\"([\\p{Graph}&&[^\"\']])+\")|(null)) \\p{javaWhitespace}*(?<whereclause>where (?<wherecol>\\w+)(\\p{javaWhitespace}*(=|<=|<|>|>=|<>)\\p{javaWhitespace}*(?<whereval>(\\d+(\\.\\d+)?)|(\"([\\p{Graph}&&[^\"\']])+\")))|(\\p{javaWhitespace}+is null)|((\\p{javaWhitespace}+is not null)))?";
}
