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

import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.result.ResultSet;
import io.github.sandeepsukumaran.davisbase.tableinformation.TableInfo;

/**
 *
 * @author Sandeep
 */
public class ReadRows {
    public ReadRows(){}

    /**
     * Read and return all rows and columns from table.
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @return ResultSet of data read.
     */
    public static ResultSet readRows(String tableName){
        ResultSet rs = null;return rs;
    }

    /**
     * Read and return rows from table
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @param colName Name of column to be used for WHERE clause.
     * @param operator Comparison operator <,=,<=,>,>=,<>
     * @return ResultSet of data read
     * @throws NoSuchColumnException
     */
    public static ResultSet readRows(String tableName, String colName, String operator) throws NoSuchColumnException{
        ResultSet rs = null;return rs;
    }
    
    /**
     * Read and return rows from table
     * @param tableName Name of table to be read from. Guaranteed to exist.
     * @param colName Name of column to be used for WHERE clause.
     * @param isnull Check for specified column IS NULL if true, IS NOT NULL if false.
     * @return ResultSet of data read
     * @throws NoSuchColumnException
     */
    public static ResultSet readRows(String tableName, String colName, Boolean isnull) throws NoSuchColumnException{
        ResultSet rs = null;return rs;
    }
    
    public static TableInfo getTableInfoFromMetadata(String tableName){
        TableInfo ti=null;
        /*
        Read all rows from davisbase_tables.tbl. If the first page is leaf, great. Else go to first leaf
        and read all rows sequentially. Each record starts with a header where the first field is the size
        of the header in int.
        */
        return ti;
    }
}
