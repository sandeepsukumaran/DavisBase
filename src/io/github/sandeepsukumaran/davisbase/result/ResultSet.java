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
package io.github.sandeepsukumaran.davisbase.result;

import java.util.ArrayList;

/**
 *
 * @author Sandeep
 */
public class ResultSet {
    public ResultSet(){
        
    }
    
    public ArrayList<ResultSetRow> getData(){return data;}
    
    public void deleteColumns(ArrayList<Integer> indices){
        data.forEach((rsr) -> {
            rsr.deleteColumns(indices);
        });
    }
    
    public ResultSet projectColumns(ArrayList<String>selectcols, ArrayList<String>tablecols){
        ArrayList<Integer> indices = new ArrayList<>();
        selectcols.forEach((selectcol) -> {
            indices.add(tablecols.indexOf(selectcol));
        });
        return this.projectColumns(indices);
    }
    
    public ResultSet projectColumns(ArrayList<Integer> indices){
        int numRows = data.size();
        for(int i=0;i<numRows;++i){
            ResultSetRow rsr = data.get(i);
            data.set(i,rsr.projectColumns(indices));
        }
        return this;
    }
    
    public void deleteColumns(ArrayList<String>selectcols, ArrayList<String>tablecols){
        ArrayList<Integer> indices = new ArrayList<>();
        selectcols.forEach((selectcol) -> {
            indices.add(tablecols.indexOf(selectcol));
        });
        this.deleteColumns(indices);
    }
    
    public ArrayList<ResultSetRow> data;
}
