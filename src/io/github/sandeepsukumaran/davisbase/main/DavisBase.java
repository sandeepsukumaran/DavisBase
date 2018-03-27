/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.github.sandeepsukumaran.davisbase.main;

import java.util.Scanner;

import io.github.sandeepsukumaran.davisbase.query.queryParser;
/**
 *
 * @author Sandeep
 */
public class DavisBase {

    private static void displaySplash(){
        System.out.println("Welcome to DavisBase. Commands end with ;");
        System.out.println("Server version: "+VERSIONSTRING+"\n");
        System.out.println(COPYRIGHTSTRING+"\n");
        System.out.println("Type \'help;\' for help.\n");
    }
    
    /**
     * @param args the command line arguments (ignored)
     */
    public static void main(String[] args) {
        //display startup splash screen
        displaySplash();
        
        String inputString;
        
        while(!exitFlag){
            //display prompt
            System.out.print(promptText);
            //Read and sanitize user input
            inputString = inputStream.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
            //Parse input and perform operations
            queryParser.parseInputCommand(inputString);
        }
        
        System.out.println("\nBye\n\n");
    }
    
    //Variable declarations
    /**< Set to true to indicate program must terminate.*/public static boolean exitFlag = false;
    /**< Variable attached to STDIN to read user inputs, delimited by ;*/static Scanner inputStream = new Scanner(System.in).useDelimiter(";");
    
    /**< Variable holding the text string displayed as prompt. Terminated with >.*/static String promptText = "davisql>";
    /**< String describing version number.*/static final String VERSIONSTRING = "0.1.0";
    /**< Copyright message.*/static final String COPYRIGHTSTRING = "Copyright (c) 2018, Sandeep Sukumaran. All rights reserved.";
    /**< Page size used by database application.*/ static final long PAGESIZE = 512;
}
