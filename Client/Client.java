package Client;

import java.util.Scanner;


public class Client {
	private static String commandSeperator = "--";
	private static Boolean isSingleCommand = false;
	
	public Client() {
		super();
	}
	
	private  static void put(String fileName) {
		
		
	}
	
	private  static void get(String fileName) {
		
		
	}
	
	private  static void list() {
		
		
	}
	
	private static void parseCommand(String command) {
//		System.err.println("Executing Command : " + command);
		String[] argumentList = command.split(" ");
		
		String type = argumentList[0]; 
		
		switch (type) {
			case "list":
				try {
					list();
					
				} catch (Exception e) {
					System.err.println("list");
				}
				
				break;
			case "get":
				try {
					get(argumentList[1]);
					
				} catch (Exception e) {
					System.err.println("get");
					}
				break;
			case "put":
				try {
					put(argumentList[1]);					
				}
				catch(IndexOutOfBoundsException e) {
					System.err.println("Wrong number of arguments specified\n");					
				}
				break;
			case "exit":
				System.out.println("Exiting...");
				break;
			default:
				System.err.println("Undefined type of command " + type);
		}
		
	}

	public static void main(String[] args) {
		for (String temp : args) {
			if (temp.equals(commandSeperator)) {
				isSingleCommand = true;
			}
		}

		if (isSingleCommand) {
			StringBuilder stringBuilder = new StringBuilder(); 
			Boolean flag = false;
			String sep = "";
			for (String temp : args) {
				if (flag) {
					stringBuilder.append(sep).append(temp);
					sep = " ";
				}
				if (temp.equals(commandSeperator)) {
					flag = true;
				}
			}
			parseCommand(stringBuilder.toString().trim());
		} else {
			// TODO: Go Console Mode
			String command = new String();
			while (!command.equals("exit")) {
				System.out.print(">>> ");
				command = new Scanner(System.in).nextLine();
				if (command.trim().equals("")) 
					continue;
				parseCommand(command.replaceAll("\\s+", " ").trim());
			}
		}
	}

}
