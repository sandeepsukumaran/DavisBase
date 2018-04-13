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

import io.github.sandeepsukumaran.davisbase.result.ResultSet;
import io.github.sandeepsukumaran.davisbase.result.ResultSetRow;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author Sandeep
 */
public class Display {
    public Display(){
        
    }
    public static void displayResults(ArrayList<String> columns, ResultSet rs){
        if(rs==null || rs.data==null){
            System.out.println("Empty set.");
            return;
        }else;
        
        int numCols = columns.size();
        ArrayList<Integer> colwidths = new ArrayList<>();
        //Initialise width to fit column names
        columns.forEach((column) -> {
            colwidths.add(column.length());
        });
        
        //Find maximum length for data in each column
        rs.data.forEach((rsr) -> {
            for(int i=0;i<numCols;++i){
                int len = rsr.contents.get(i).length(); 
                if(len > colwidths.get(i))
                    colwidths.set(i, len);
            }
        });
        
        int totW=0;
        for(int i=0;i<numCols;++i){
            int newW = colwidths.get(i)+2;
            colwidths.set(i, newW);
            totW += newW;
        }
        
        //print separator line of +,- 's
        for(int i=0;i<numCols;++i){
            System.out.print("+");
            for(int j=0;j<colwidths.get(j);++j)
                System.out.print("-");
        }
        System.out.println("+");
        
        //print column names
        for(int i=0;i<numCols;++i){
            System.out.print("|");
            System.out.print(StringCenter.center(columns.get(i),colwidths.get(i)));
        }
        System.out.println("|");
        
        //print separator line of +,- 's
        for(int i=0;i<numCols;++i){
            System.out.print("+");
            for(int j=0;j<colwidths.get(j);++j)
                System.out.print("-");
        }
        System.out.println("+");
        
        //print data
        for(ResultSetRow rsr:rs.data){
            for(int i=0;i<numCols;++i){
                System.out.print("|");
                System.out.print(StringCenter.center(rsr.contents.get(i),colwidths.get(i)));
            }
            System.out.println("|");
        }
        
        //print separator line of +,- 's
        for(int i=0;i<numCols;++i){
            System.out.print("+");
            for(int j=0;j<colwidths.get(j);++j)
                System.out.print("-");
        }
        System.out.println("+");
        
        System.out.println(Integer.toString(rs.data.size())+" rows in set.");
    }
}
