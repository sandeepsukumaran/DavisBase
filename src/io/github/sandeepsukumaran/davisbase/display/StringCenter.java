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
package io.github.sandeepsukumaran.davisbase.display;

/**
 *
 * @author Sandeep
 */
public class StringCenter {
    public StringCenter(){}
    
    public static String center(String str, int size){
      if(str == null || size <= 0){
          return str;
      }else{}
      int strLen = str.length();
      int pads = size - strLen;
      if(pads <= 0){
          return str;
      }else{}
      str = leftPad(str, strLen + pads / 2,' ');
      str = rightPad(str, size, ' ');
      return str;
    }
    
    public static String rightPad(String str, int size, char padChar) {
        if(str == null){
            return null;
        }else{}
        int pads = size - str.length();
        if(pads <= 0){
            return str; // returns original String when possible
        }else{}
        if(pads > PAD_LIMIT){
            return rightPad(str, size, String.valueOf(padChar));
        }else{}
        return str.concat(padding(pads, padChar));
    }
    
    public static String rightPad(String str, int size, String padStr) {
        if(str == null){
            return null;
        }else{}
        if(isEmpty(padStr)){
            padStr = " ";
        }else{}
        int padLen = padStr.length();
        int strLen = str.length();
        int pads = size - strLen;
        if(pads <= 0){
            return str; // returns original String when possible
        }else{}
        if(padLen == 1 && pads <= PAD_LIMIT){
            return rightPad(str, size, padStr.charAt(0));
        }else{}

        if(pads == padLen){
            return str.concat(padStr);
        }else if(pads < padLen){
            return str.concat(padStr.substring(0, pads));
        }else{
            char[] padding = new char[pads];
            char[] padChars = padStr.toCharArray();
            for (int i = 0; i < pads; i++) {
                padding[i] = padChars[i % padLen];
            }
            return str.concat(new String(padding));
        }
    }
    
    public static String leftPad(String str, int size, char padChar) {
        if(str == null){
            return null;
        }else{}
        int pads = size - str.length();
        if(pads <= 0){
            return str; // returns original String when possible
        }else{}
        if(pads > PAD_LIMIT){
            return leftPad(str, size, String.valueOf(padChar));
        }else{}
        return padding(pads, padChar).concat(str);
    }
    
    public static String leftPad(String str, int size, String padStr) {
        if(str == null){
            return null;
        }else{}
        if(isEmpty(padStr)){
            padStr = " ";
        }else{}
        int padLen = padStr.length();
        int strLen = str.length();
        int pads = size - strLen;
        if(pads <= 0){
            return str; // returns original String when possible
        }else{}
        if(padLen == 1 && pads <= PAD_LIMIT){
            return leftPad(str, size, padStr.charAt(0));
        }else{}

        if(pads == padLen){
            return padStr.concat(str);
        }else if (pads < padLen){
            return padStr.substring(0, pads).concat(str);
        }else{
            char[] padding = new char[pads];
            char[] padChars = padStr.toCharArray();
            for(int i = 0; i < pads; i++){
                padding[i] = padChars[i % padLen];
            }
            return new String(padding).concat(str);
        }
    }
    
    public static boolean isEmpty(String str){
        return str == null || str.length() == 0;
    }
    
    private static String padding(int repeat, char padChar) throws IndexOutOfBoundsException{
        if(repeat < 0){
            throw new IndexOutOfBoundsException("Cannot pad a negative amount: " + repeat);
        }else{}
        final char[] buf = new char[repeat];
        for(int i = 0; i < buf.length; i++){
            buf[i] = padChar;
        }
        return new String(buf);
    }
    
    private static final int PAD_LIMIT = 8192;
}
