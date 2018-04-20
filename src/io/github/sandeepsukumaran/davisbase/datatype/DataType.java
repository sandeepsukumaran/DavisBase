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

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    public int getDataTypeAsInt(){
        return type;
    }
    public static int getDataTypeAsInt(String dType){
        return DATATYPEINDEX.get(dType);
    }
    public int getDataTypeSize(){
        return size;
    }
    public static int getDataTypeSize(String dType){
        return DATATYPESIZE.get(DATATYPEINDEX.get(dType));
    }
    public static int getDataTypeSize(byte b){
        switch(b){
            case 0x00:
            case 0x04:
                return 1;
            case 0x01:
            case 0x05:
                return 2;
            case 0x02:
            case 0x06:
            case 0x08:
                return 4;
            case 0x03:
            case 0x07:
            case 0x09:
            case 0x0a:
            case 0x0b:
                return 8;
            case 0x0c:
                return 1;
            default:
                return b - 0x0c;
        }
    }
    @Override
    public String toString(){
        switch(type){
            case 1: return "tinyint";
            case 2: return "smallint";
            case 3: return "int";
            case 4: return "bigint";
            case 5: return "real";
            case 6: return "double";
            case 7: return "datetime";
            case 8: return "date";
            case 9: return "text";
        }
        return "";
    }
    public static String dataAsString(int type, int val){
        switch(type){
            case 1:
            case 2:
            case 3: return Integer.toString(val);
        }
        return "";
    }
    public static String dataAsString(int type, long val){
        switch(type){
            case 4: return Long.toString(val);
            case 7: return (new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").format(new Date(val)));
            case 8: return (new SimpleDateFormat("yyyy-MM-dd").format(new Date(val)));
        }
        return "";
    }
    public static String dataAsString(int type, double val){
        switch(type){
            case 5: return Float.toString((float)val);
            case 6: return Double.toString(val);
        }
        return "";
    }
    public static boolean validData(String data, String dtypeName){
        try{
            switch(dtypeName){
            case "tinyint": Byte.parseByte(data);return true;
            case "smallint": Short.parseShort(data);return true;
            case "int": Integer.parseInt(data);return true;
            case "bigint": Long.parseLong(data);return true;
            case "real": Float.parseFloat(data);return true;
            case "double": Double.parseDouble(data);return true;
            case "datetime":
                Date dt = SIMPLEDATETIMEFORMAT.parse(data,new ParsePosition(0));
                return (dt!=null);
            case "date":
                Date d = SIMPLEDATEFORMAT.parse(data,new ParsePosition(0));
                return (d!=null);
            default: return true;// TEXT trivially true
            }
        }catch(NumberFormatException nfe){
            return false;
        }
    }
    public static Object valueToObject(String data, DataType dtype){
        switch(dtype.type){
            case 1: return Byte.parseByte(data);
            case 2: return Short.parseShort(data);
            case 3: return Integer.parseInt(data);
            case 4: return Long.parseLong(data);
            case 5: return Float.parseFloat(data);
            case 6: return Double.parseDouble(data);
            case 7:
                Date dt = SIMPLEDATETIMEFORMAT.parse(data,new ParsePosition(0));
                if(dt!=null)
                    return (Long)dt.getTime();
                else
                    return null;
            case 8:
                Date d = SIMPLEDATEFORMAT.parse(data,new ParsePosition(0));
                if(d!=null)
                    return (Long)d.getTime();
                else
                    return null;
            case 9:
                return data.substring(1,data.length()-1); //get rid of quotes
        }
        return null;
    }
    public static boolean validData(String data, DataType dtype){
        try{
            switch(dtype.type){
            case 1: Byte.parseByte(data);return true;
            case 2: Short.parseShort(data);return true;
            case 3: Integer.parseInt(data);return true;
            case 4: Long.parseLong(data);return true;
            case 5: Float.parseFloat(data);return true;
            case 6: Double.parseDouble(data);return true;
            case 7:
                Date dt = SIMPLEDATETIMEFORMAT.parse(data,new ParsePosition(0));
                return (dt!=null);
            case 8:
                Date d = SIMPLEDATEFORMAT.parse(data,new ParsePosition(0));
                return (d!=null);
            default: return true;// TEXT trivially true
            }
        }catch(NumberFormatException nfe){
            return false;
        }
    }
    public static boolean isNullSerialCode(Byte serialcode){
        return (serialcode <= 0x03)||(serialcode == 0x0c);
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
    public static final SimpleDateFormat SIMPLEDATETIMEFORMAT = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
    public static final SimpleDateFormat SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd");
}
