package com.iiq.rtbEngine.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class FilesUtil {
	
	
	/**
	 * API for creating or overriding a new file in the system
	 * @param filePath - the full path of the file to be created
	 */
	public static void createNewFile(String filePath) {
		try {
			Writer fileWriter = new FileWriter(filePath, /*overwrites file if exist*/ false);
			fileWriter.write("");
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("An error occured while trying to create file "+filePath);
			e.printStackTrace();
		}
	}
	
	/**
	 * API for writing a single line (with line end character) into a file
	 * @param filePath - the full path of the file to read
	 * @param line - the line to be written
	 */
	public static void writeLineToFile(String filePath, String line) {
		try {
			FileWriter fileWriter = new FileWriter(filePath, true);
			fileWriter.write(line+"\n");
			fileWriter.close();
		} catch (IOException e) {
			System.out.println("An error occurred while trying to write line "+line+" to file "+filePath);
			e.printStackTrace();
		}
	}
	
	/**
	 * API for reading all lines from a file
	 * @param filePath - the full path of the file to read
	 * @return a list of lines read from the file
	 */
	public static List<String> readLinesFromFile(String filePath) {
		List<String> fileLines = new ArrayList<>();
		
		try {
			File file = new File(filePath);
			Scanner fileReader = new Scanner(file);
			while (fileReader.hasNextLine()) {
				fileLines.add(fileReader.nextLine());
			}
			fileReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred while trying to read lines from file "+filePath);
			e.printStackTrace();
		}
		
		return fileLines;
	}

}
