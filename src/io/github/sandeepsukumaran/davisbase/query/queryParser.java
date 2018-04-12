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

import io.github.sandeepsukumaran.davisbase.exception.InvalidQuerySyntaxException;
import io.github.sandeepsukumaran.davisbase.main.DavisBase;
import io.github.sandeepsukumaran.davisbase.exception.NoDatabaseSelectedException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException;
import io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException;
/**
 *
 * @author Sandeep
 */
public class queryParser {
    /**
     * Parse the received user input and perform operations.
     * @param inputCommand String input by user from STDIN.
     * @throws io.github.sandeepsukumaran.davisbase.exception.NoDatabaseSelectedException
     * @throws io.github.sandeepsukumaran.davisbase.exception.InvalidQuerySyntaxException
     * @throws io.github.sandeepsukumaran.davisbase.exception.NoSuchTableException
     * @throws io.github.sandeepsukumaran.davisbase.exception.NoSuchColumnException
     */
    public static void parseInputCommand(String inputCommand) throws NoDatabaseSelectedException,InvalidQuerySyntaxException,NoSuchTableException,NoSuchColumnException{
        switch(inputCommand){
            case EXIT_COMMAND:
                io.github.sandeepsukumaran.davisbase.main.DavisBase.exitFlag = true;
                return;
            case HELP_COMMAND:
                displayHelpText();
                return;
            case SHOW_TABLES_COMMAND:
                showTables();
                return;
            case VERSION_COMMAND:
                System.out.println("Server version: "+DavisBase.VERSIONSTRING+"\n");
                return;
        }
        //inputCommand is not one of the above cases
        if (inputCommand.matches(SELECT_QUERY))
            new selectQueryHandler(inputCommand).execute();
        else if (inputCommand.matches(INSERT_QUERY))
            new insertQueryHandler(inputCommand).execute();
        else if(inputCommand.matches(UPDATE_QUERY))
            new updateQueryHandler(inputCommand).execute();
        else
            throw new InvalidQuerySyntaxException();
    }
    
    private static void showDatabases(){
        return;
    }
    
    /**
     * List the tables in the active database.
     * @throws NoDatabaseSelectedException 
     */
    private static void showTables() throws NoDatabaseSelectedException{
        if (DavisBase.activeDatabase == null)
            throw new NoDatabaseSelectedException();
        return;
    }
    private static void displayHelpText(){
        System.out.println("\n\nFor developer information visit:\n\thttps://sandeepsukumaran.github.io/DavisBase\n\n");
        System.out.println("List of all supported commands:\nNote that all text commands must be first on line and end with \';\'");
        System.out.println("Commands are case insensitive");
        //System.out.println("\tUSE DATABASE database_name;                      Changes current database.");
        //System.out.println("\tCREATE DATABASE database_name;                   Creates an empty database.");
        //System.out.println("\tSHOW DATABASES;                                  Displays all databases.");
        //System.out.println("\tDROP DATABASE database_name;                     Deletes a database.");
        System.out.println("\tSHOW TABLES;                                     Displays all tables in current database.");
        //System.out.println("\tDESC table_name;                                 Displays table schema.");
        System.out.println("\tCREATE TABLE table_name (                        Creates a table in current database.");
        System.out.println("\t\t<column_name> <datatype> [PRIMARY KEY | NOT NULL]");
        System.out.println("\t\t...);");
        System.out.println("\tDROP TABLE table_name;                           Deletes a table data and its schema.");
        System.out.println("\tSELECT <column_list> FROM table_name             Display records whose rowid is <id>.");
        System.out.println("\t\t[WHERE rowid = <value>];");
        System.out.println("\tINSERT INTO table_name                           Inserts a record into the table.");
        System.out.println("\t\t[(<column1>, ...)] VALUES (<value1>, <value2>, ...);");
        //System.out.println("\tDELETE FROM table_name [WHERE condition];        Deletes a record from a table.");
        System.out.println("\tUPDATE table_name SET <conditions>               Updates a record from a table.");
        System.out.println("\t\t[WHERE condition];");
        System.out.println("\tVERSION;                                         Display current database engine version.");
        System.out.println("\tHELP;                                            Displays help information");
        System.out.println("\tEXIT;                                            Exits the program\n\n");
    }
    
    //Keywords list
    static final String EXIT_COMMAND = "exit";
    static final String HELP_COMMAND = "help";
    static final String SHOW_TABLES_COMMAND = "show tables";
    static final String VERSION_COMMAND = "version";
    //static final String SHOW_DATABASES_COMMAND = "show databases";
    
    //regex for valid commands
    static final String SELECT_QUERY = "select \\*|(\\w+(\\p{javaWhitespace}?,\\p{javaWhitespace}?\\w+)*) from \\w+ (where \\w+\\p{javaWhitespace}?=\\p{javaWhitespace}?((\\d+(\\.\\d+)?)|\"(\\p{Punct}&&[^\"\'])+\"))?;";
    static final String INSERT_QUERY = "insert into \\w+\\p{javaWhitespace}*(\\(\\w+(\\p{javaWhitespace}*,\\p{javaWhitespace}*\\w+)*\\))?\\p{javaWhitespace}*values\\p{javaWhitespace}*\\((\\d+(\\.\\d+)?)|(\"(\\p{Punct}&&[^\"\'])+\")(\\p{javaWhitespace}*,\\p{javaWhitespace}*(\\d+(\\.\\d+)?)|\"(\\p{Punct}&&[^\"\'])+\")*\\);";
    static final String UPDATE_QUERY = "update \\w+ set \\w+\\p{javaWhitespace}*=\\p{javaWhitespace}*(\\d+(\\.\\d+)?)|(\"(\\p{Punct}&&[^\"\'])+\") \\p{javaWhitespace}*(where \\w+(\\p{javaWhitespace}*=\\p{javaWhitespace}*((\\d+(\\.\\d+)?)|(\"(\\p{Punct}&&[^\"\'])+\")))|(\\p{javaWhitespace}+is null)|((\\p{javaWhitespace}+is not null)))?;";
}