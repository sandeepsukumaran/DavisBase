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
package io.github.sandeepsukumaran.davisbase.helpermethods;

import java.util.ArrayList;

/**
 *
 * @author Sandeep
 */
public class HelperMethods {
    public static ArrayList<Object> uniqueArrayList(ArrayList<Object> ip){
        ArrayList<Object> result = new ArrayList<>();
        for(Object e:ip)
            if(!result.contains(e))
                result.add(e);
        return result;
    }
    public static ArrayList<String> uniqueStringArrayList(ArrayList<String> ip){
        ArrayList<String> result = new ArrayList<>();
        for(String e:ip)
            if(!result.contains(e))
                result.add(e);
        return result;
    }
}
