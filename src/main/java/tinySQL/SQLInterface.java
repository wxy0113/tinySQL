package main.java.tinySQL;

import main.java.storageManager.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SQLInterface {
    public SQLInterface(){}
    public void run() {
        boolean quit = false;
        //Parser parser = new Parser();
        PhysicalQuery physicalQuery = new PhysicalQuery();
        Scanner scan = new Scanner(System.in);
        while (!quit) {
            System.out.println("--------------------------TinySQL-----------------------------");
            System.out.println("Author: Xiaoyu Wang, Bohuai Jiang");
            System.out.println("--------------------------------------------------------------");
            System.out.println("Choose one of the following actions: ");
            System.out.println("1. Input Command Manually");
            System.out.println("2. Execute SQL script file");
            System.out.println("3. Quit");
            String mode = scan.nextLine();
            switch (mode) {
                case "1":
                    System.out.println("--------------------------CLI Mode-----------------------------");
                    System.out.println("Input quit to exit CLI mode.");
                    System.out.println("Please input command:");
                    String command = scan.nextLine();
                    while (!command.equalsIgnoreCase("quit")) {
                        physicalQuery.execute(command);
                        System.out.println("--------------------------CLI Mode-----------------------------");
                        System.out.println("Input \\\"quit\\\" to exit CLI mode\\n\\nPlease input command:");
                        command = scan.nextLine();
                    }
                    break;
                case "2":
                    System.out.println("--------------------------File Mode-----------------------------");
                    System.out.println("Input quit to exit CLI mode.");
                    System.out.println("Please input FILE NAME:");
                    String name = scan.nextLine();
                    while (!name.equalsIgnoreCase("quit")) {
                        File file = new File(name);
                        List<String> command_list = fileReader(file);
                        for (String s: command_list) {
                            physicalQuery.execute(s);
                        }
                        System.out.println("--------------------------File Mode-----------------------------");
                        System.out.println("Input quit to exit CLI mode.");
                        System.out.println("Please input FILE NAME:");
                        name = scan.nextLine();
                    }
                    break;
                case "3":
                    quit = true;
                    System.out.println("Thank you for using Tiny-SQL!");
                    System.out.println("-----------------------------");
                    break;
                default :
                    System.out.println("Invalid Command, please input again: ");

            }
        }
    }
    public static List<String> fileReader(File file){
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new FileReader(file));
            List<String> inputLines=new ArrayList<String>();
            String cmd;
            while((cmd=reader.readLine())!=null){
                inputLines.add(cmd);
            }
            reader.close();
            return inputLines;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }finally {
            if(reader!=null)
                try{
                    reader.close();
                }catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }
}
