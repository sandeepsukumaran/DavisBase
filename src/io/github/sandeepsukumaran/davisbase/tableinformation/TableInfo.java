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
package io.github.sandeepsukumaran.davisbase.tableinformation;

/**
 *
 * @author Sandeep
 */
public class TableInfo {
    public TableInfo(){
        
    }

    /**< Name of table.*/public String tableName;
    /**< Number of records in table.*/public int recordCount;
    /**< Page number of root page. Will be 1 or 3.*/public int rootPage;
    /**< Length of a record>*/public int avgLength;
    /**< row_id of table entry in meta-data table.*/public int row_id;
}
