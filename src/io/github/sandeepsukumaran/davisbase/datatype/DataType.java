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
package io.github.sandeepsukumaran.davisbase.datatype;

import java.util.HashMap;

/**
 *
 * @author Sandeep
 */
public class DataType {
    public DataType(){
        type = -1;
        size = -1;
    }
    public DataType(String dataType){
        type = DATATYPEINDEX.get(dataType);
        size = DATATYPESIZE.get(type);
    }
    int type;
    int size;

    /**< Sizes of various data types in bytes.*/public static final HashMap<Integer,Integer> DATATYPESIZE;
    /**< Internal index of data type.*/public static final HashMap<String,Integer> DATATYPEINDEX;
    static{
        DATATYPESIZE = new HashMap<>();
        DATATYPESIZE.put(1,1);
        DATATYPESIZE.put(2,3);
        DATATYPESIZE.put(3,4);
        DATATYPESIZE.put(4,8);
        DATATYPESIZE.put(5,4);
        DATATYPESIZE.put(6,8);
        DATATYPESIZE.put(7,8);
        DATATYPESIZE.put(8,8);
        DATATYPESIZE.put(9,8);
        DATATYPEINDEX = new HashMap<>();
        DATATYPEINDEX.put("tinyint",1);
        DATATYPEINDEX.put("smallint",2);
        DATATYPEINDEX.put("int",3);
        DATATYPEINDEX.put("bigint",4);
        DATATYPEINDEX.put("real",5);
        DATATYPEINDEX.put("double",6);
        DATATYPEINDEX.put("datetime",7);
        DATATYPEINDEX.put("date",8);
        DATATYPEINDEX.put("text",9);
    }
}
