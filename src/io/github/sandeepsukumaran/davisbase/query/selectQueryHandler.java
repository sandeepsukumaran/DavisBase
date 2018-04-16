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

import io.github.sandeepsukumaran.davisbase.display.Display;
import io.github.sandeepsukumaran.davisbase.exception.FileAccessException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidDataType;
import io.github.sandeepsukumaran.davisbase.exception.InvalidQuerySyntaxException;
import io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException;
import io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.result.ResultSet;
import io.github.sandeepsukumaran.davisbase.tree.ReadRows;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Sandeep
 */
public class selectQueryHandler {
    public selectQueryHandler(String inputQuery){
        query = inputQuery;
        selectAllPattern = Pattern.compile(SELECT_ALL_QUERY);
        selectPattern = Pattern.compile(SELECT_QUERY);
        selectAllMatcher = selectAllPattern.matcher(query);
        selectAllMatcher.matches();
        selectMatcher = selectPattern.matcher(query);
        selectMatcher.matches();
    }
    
    /**
     * Execute Select Query
     * @throws NoSuchTableException
     * @throws NoSuchColumnException
     * @throws InvalidQuerySyntaxException
     * @throws io.github.sandeepsukumaran.davisbase.exception.FileAccessException
     * @throws io.github.sandeepsukumaran.davisbase.exception.MissingTableFileException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidDataType
     * @throws java.io.IOException
     * @throws java.io.FileNotFoundException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidTableInformationException
     */
    public void execute() throws NoSuchTableException,NoSuchColumnException,InvalidQuerySyntaxException, FileAccessException, MissingTableFileException, InvalidDataType, IOException, FileNotFoundException, InvalidTableInformationException{
        if (selectAllMatcher.matches()){
            //select all query
            selectAllQueryExecute();
        }else if(selectMatcher.matches()){
            //select columns query
            selectQueryExecute();
        }else
            //should not happen. Must be caught in query parser itself.
            throw new InvalidQuerySyntaxException();
    }
    
    private void selectAllQueryExecute() throws NoSuchTableException, NoSuchColumnException, FileAccessException, MissingTableFileException, InvalidDataType, IOException, FileNotFoundException, InvalidTableInformationException{
        ArrayList<String> tableNames = DavisBase.getTableNames();
        String tableName = selectAllMatcher.group("tablename"); System.out.println(tableName);
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        ArrayList<String> tablecols = DavisBase.getTableColumns(tableName);
        String whereclause = selectAllMatcher.group("whereclause");
        if (whereclause==null){
            //select all rows all columns
            Display.displayResults(tablecols,ReadRows.readRows(tableName));
        }else{
            //where condition exists
            whereclause = whereclause.substring(5).trim();
            int pos = whereclause.indexOf('=');
            if (pos != -1){
                //comparison operator used in query
                String colName;
                if ((whereclause.charAt(pos-1)!='>') && (whereclause.charAt(pos-1)!='<')){
                    colName = whereclause.substring(0,pos).trim();
                    Display.displayResults(tablecols,ReadRows.readRows(tableName,colName,"=",whereclause.substring(pos+1,whereclause.length()-1)));
                }else{
                    colName = whereclause.substring(0,pos-1).trim(); //pos is location of =
                    Display.displayResults(tablecols,ReadRows.readRows(tableName,colName,whereclause.substring(pos-1,pos+1),whereclause.substring(pos+1,whereclause.length()-1)));
                }
            }else if(whereclause.contains("<>")){
                String colName = whereclause.substring(0,whereclause.indexOf("<>")).trim();
                Display.displayResults(tablecols,ReadRows.readRows(tableName,colName,"<>",whereclause.substring(whereclause.indexOf("<>")+2,whereclause.length()-1)));
            }else if(whereclause.contains("is null")){
                String colName = whereclause.substring(0,whereclause.indexOf("is null")).trim();
                Display.displayResults(tablecols,ReadRows.readRows(tableName,colName,true));
            }else{
                String colName = whereclause.substring(0,whereclause.indexOf("is not null")).trim();
                Display.displayResults(tablecols,ReadRows.readRows(tableName,colName,false));
            }
        }
    }
    
    private void selectQueryExecute() throws NoSuchTableException, NoSuchColumnException, FileAccessException, MissingTableFileException, InvalidDataType, IOException, FileNotFoundException, InvalidTableInformationException{
        ArrayList<String> tableNames = DavisBase.getTableNames();
        String tableName = selectMatcher.group("tablename");
        if (!tableNames.contains(tableName))
            throw new NoSuchTableException(tableName);
        else;
        ArrayList<String> selectcols = new ArrayList<>(Arrays.asList(selectMatcher.group("columnnames").split(",")));
        ArrayList<String> tablecols = DavisBase.getTableColumns(tableName);
        
        if (!tablecols.containsAll(selectcols)){
            //not all requested columns are in the table
            selectcols.removeAll(tablecols);
            throw new NoSuchColumnException(selectcols.get(0),tableName);
        }else;
        
        String whereclause = selectMatcher.group("whereclause");
        if (whereclause==null){
            //select all rows
            ResultSet rs = ReadRows.readRows(tableName);
            Display.displayResults(selectcols,rs.projectColumns(selectcols,tablecols));
        }else{
            //where condition exists
            whereclause = whereclause.substring(5).trim();
            int pos = whereclause.indexOf('=');
            if (pos != -1){
                //comparison operator used in query
                String colName;
                if ((whereclause.charAt(pos-1)!='>') && (whereclause.charAt(pos-1)!='<')){
                    colName = whereclause.substring(0,pos).trim();
                    ResultSet rs = ReadRows.readRows(tableName,colName,"=",whereclause.substring(pos+1,whereclause.length()-1));
                    Display.displayResults(selectcols,rs.projectColumns(selectcols,tablecols));
                }else{
                    colName = whereclause.substring(0,pos-1).trim(); //pos is location of =
                    ResultSet rs = ReadRows.readRows(tableName,colName,whereclause.substring(pos-1,pos+1),whereclause.substring(pos+1,whereclause.length()-1));
                    Display.displayResults(selectcols,rs.projectColumns(selectcols,tablecols));
                }
            }else if(whereclause.contains("<>")){
                String colName = whereclause.substring(0,whereclause.indexOf("<>")).trim();
                ResultSet rs = ReadRows.readRows(tableName,colName,"<>",whereclause.substring(whereclause.indexOf("<>")+2,whereclause.length()-1));
                Display.displayResults(selectcols,rs.projectColumns(selectcols,tablecols));
            }else if(whereclause.contains("is null")){
                String colName = whereclause.substring(0,whereclause.indexOf("is null")).trim();
                ResultSet rs = ReadRows.readRows(tableName,colName,true);
                Display.displayResults(selectcols,rs.projectColumns(selectcols,tablecols));
            }else{
                String colName = whereclause.substring(0,whereclause.indexOf("is not null")).trim();
                ResultSet rs = ReadRows.readRows(tableName,colName,false);
                Display.displayResults(selectcols,rs.projectColumns(selectcols,tablecols));
            }
        }
    }
    
    private final Pattern selectAllPattern;
    private final Pattern selectPattern;
    private final Matcher selectAllMatcher;
    private final Matcher selectMatcher;
    private final String query;
    private final String SELECT_ALL_QUERY = "^select \\* from (?<tablename>\\w+)(?<whereclause>where\\p{javaWhitespace}+\\w+(\\p{javaWhitespace}*(=|<=|<|>|>=|<>)\\p{javaWhitespace}*((\\d+(\\.\\d+)?)|\"([\\p{Graph}&&[^\"\']])+\"))|(\\p{javaWhitespace}+is null)|(\\p{javaWhitespace}+is not null))?$";
    private static final String SELECT_QUERY = "^select (?<columnnames>\\w+(\\p{javaWhitespace}?,\\p{javaWhitespace}?\\w+)*) from (<?tablename>\\w+)(?<whereclause>where\\p{javaWhitespace}+\\w+(\\p{javaWhitespace}*(=|<=|<|>|>=|<>)\\p{javaWhitespace}*((\\d+(\\.\\d+)?)|\"([\\p{Graph}&&[^\"\']])+\"))|(\\p{javaWhitespace}+is null)|(\\p{javaWhitespace}+is not null))?$";
}
