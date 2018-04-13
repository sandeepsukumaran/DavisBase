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
public class ResultSetRow {
    public ResultSetRow(){
        contents = new ArrayList<>();
    }
    public ArrayList<String> getRowContents(){
        return contents;
    }
    
    public void deleteColumns(ArrayList<Integer> indices){
        
    }
    public ResultSetRow projectColumns(ArrayList<Integer> indices){
        ResultSetRow res = new ResultSetRow();
        indices.forEach((index) -> {
            res.contents.add(this.contents.get(index));
        });
        return res;
    }
    
    public ArrayList<String> contents;
}
