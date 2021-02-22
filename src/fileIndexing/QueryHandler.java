package fileIndexing;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class QueryHandler {
	public String commandType;
	public boolean isExit;

	public QueryHandler(String type) {
		this.commandType = type;
	}

	public static void createSequence(String tableName, String columnName, int seed, int incrementBy)
			throws IOException {
		String sequenceName = Utility.getSequenceName(tableName, columnName);
		try {
			RandomAccessFile file = new RandomAccessFile(Utility.getFilePath("seq", sequenceName), "rw");
			file.setLength(0);
			file.setLength(MyDatabase.pageSize);
			file.seek(0);
			file.writeInt(seed);
			file.writeInt(incrementBy);
			file.writeInt(0);
			if (file != null)
				file.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Utility to print table
	 * 
	 * @param table
	 */
	public static void printTable(List<LinkedHashMap<String, ArrayList<String>>> table) {
		if (table.isEmpty()) {
			System.out.println("Table is empty! 0 rows were returned");
			return;
		}

		int size[] = new int[table.get(0).size()];
		Arrays.fill(size, -1);
		int k = 0;
		for (LinkedHashMap<String, ArrayList<String>> x : table) {
			k = 0;
			for (String y : x.keySet()) {
				if (size[k] < y.length()) {
					size[k] = y.length();
				}
				if (size[k] < x.get(y).get(0).length()) {
					size[k] = x.get(y).get(0).length();
				}
				k++;
			}
		}
		for (int i : size)
			System.out.print("+" + Utility.displayLine("-", i));
		System.out.print("+\n");
		k = 0;
		for (String x : table.get(0).keySet())
			System.out.print("|" + Utility.format(x.toUpperCase(), size[k++]));
		System.out.print("|\n");
		for (int i : size)
			System.out.print("+" + Utility.displayLine("-", i));
		System.out.print("+\n");
		for (LinkedHashMap<String, ArrayList<String>> x : table) {
			k = 0;
			for (String y : x.keySet())
				System.out.print("|" + Utility.format(x.get(y).get(0), size[k++]));
			System.out.print("|\n");
			for (int i : size)
				System.out.print("+" + Utility.displayLine("-", i));
			System.out.print("+\n");
		}
	}

	public static void printHistory() {
		int len = MyDatabase.history.size();
		if (len == 0) {
			System.out.println("No history found!");
			return;
		}
		for (int i = 0; i < len; i++) {
			System.out.println((i + 1) + "\t" + MyDatabase.history.get(i));
		}
	}

	public static void showTables() {
		ArrayList<String> tableList = new ArrayList<String>();
		File folder = new File(Utility.getOSPath(new String[] { MyDatabase.tableLocation, MyDatabase.userDataFolder }));
		File[] listOfFiles = folder.listFiles();
		tableList.add(MyDatabase.masterTableName);
		tableList.add(MyDatabase.masterColumnTableName);

		for (int i = 0; i < listOfFiles.length; i++) {
			tableList.add(listOfFiles[i].getName().replaceAll(".tbl", ""));
		}
		System.out.println("+-----+--------------------+");
		System.out.println("|ROWID|     TABLE_NAME     |");
		System.out.println("+-----+--------------------+");
		for (int i = 0; i < tableList.size(); i++) {
			System.out.println("|    " + i + "|  " + tableList.get(i)
					+ Utility.displayLine(" ", 18 - (tableList.get(i).length())) + "|");
			System.out.println("+-----+--------------------+");
		}
	}

	public static boolean dropTable(String tableName) {
		boolean isDropped = false;
		int flag = 1;
		RandomAccessFile dropTableFile = null;
		File folder = new File(Utility.getOSPath(new String[] { MyDatabase.tableLocation, MyDatabase.userDataFolder }));
		File[] listOfFiles = folder.listFiles();
		RandomAccessFile mDBColumnFile, mDBtableFile;
		for (int i = 0; i < listOfFiles.length; i++)
			if (listOfFiles[i].getName().equals(tableName + ".tbl")) {
				flag = 0;
			}
		if (flag == 1) {
			System.out.println("Table does not exist");
			return false;
		}

		try {
			mDBColumnFile = new RandomAccessFile(Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw");
			mDBtableFile = new RandomAccessFile(Utility.getFilePath("master", MyDatabase.masterTableName), "rw");
			dropTableFile = new RandomAccessFile(Utility.getFilePath("user", tableName), "rw");
		} catch (FileNotFoundException e1) {
			System.out.println(" Table file not found");
			return false;
		}
		try {
			dropTableFile.setLength(0);
			if (dropTableFile != null)
				dropTableFile.close();
			File f = new File(Utility.getFilePath("user", tableName));
			f.delete();
			System.out.println("Table Dropped");
		} catch (IOException e) {
			return false;
		}
		BTree tableBTree = new BTree(mDBtableFile, MyDatabase.masterTableName, false, true);
		BTree columnBTree = new BTree(mDBColumnFile, MyDatabase.masterColumnTableName, true, false);
		ArrayList<String> arryL = new ArrayList<String>();
		arryL.add(new Integer(1).toString()); // search cond col ordinal
		// position
		arryL.add("text"); // search cond col data type
		arryL.add(tableName); // search cond col value
		List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.searchWithNonPK(arryL);
		for (LinkedHashMap<String, ArrayList<String>> map : op) {
			Integer rowId = Integer.parseInt(map.get("rowid").get(0));
			LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
			ArrayList<String> array = new ArrayList<String>();
			array.add("int");
			array.add(rowId.toString());
			token.put("rowid", new ArrayList<String>(array));
			isDropped = tableBTree.deleteRecord(token);

		}
		arryL = new ArrayList<String>();
		arryL.add(new Integer(1).toString()); // search cond col ordinal
		// position
		arryL.add("text"); // search cond col data type
		arryL.add(tableName); // search cond col value
		List<LinkedHashMap<String, ArrayList<String>>> opp = columnBTree.searchWithNonPK(arryL);
		for (LinkedHashMap<String, ArrayList<String>> map : opp) {
			Integer rowId = Integer.parseInt(map.get("rowid").get(0));
			LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
			ArrayList<String> array = new ArrayList<String>();
			array.add("int");
			array.add(rowId.toString());
			token.put("rowid", new ArrayList<String>(array));
			isDropped = columnBTree.deleteRecord(token);
		}
		return isDropped;
	}

	public static void update(String tableName, String columnName) throws IOException {
		String sequenceName = Utility.getSequenceName(tableName, columnName);
		int value = 0;
		try {
			RandomAccessFile file = new RandomAccessFile(Utility.getFilePath("seq", sequenceName), "rw");
			file.seek(0);
			int seed = file.readInt();
			int incrementBy = file.readInt();
			value = file.readInt();

			if (value == 0)
				value = seed;
			else
				value += incrementBy;

			file.seek(0);
			file.writeInt(seed);
			file.writeInt(incrementBy);
			file.writeInt(value);
			if (file != null)
				file.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return;
	}

	public static int nextValueInSequence(String tableName, String columnName) throws IOException {
		String sequenceName = Utility.getSequenceName(tableName, columnName);

		int value = 0;
		try {
			RandomAccessFile seqFile = new RandomAccessFile(Utility.getFilePath("seq", sequenceName), "rw");
			seqFile.seek(0);
			int seed = seqFile.readInt();
			int incrementBy = seqFile.readInt();
			// System.out.println("increment by: " + incrementBy + " " + seed);
			value = seqFile.readInt();
			if (value == 0)
				value = seed;
			else
				value += incrementBy;
			if (seqFile != null)
				seqFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	public static void help() {
		System.out.println(Utility.displayLine("*", 170));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println(
				"\tCREATE TABLE table_name (row_id INT PRIMARY KEY, name text);                        Create a new table schema if not already exist.");
		System.out.println(
				"\tSHOW TABLES;                                                                        Displays a list of all tables in the Database.");
		System.out.println(
				"\tDROP TABLE table_name;                                                              Remove table data and its schema.");
		System.out.println(
				"\tSELECT * FROM table_name;                                                           Display all records in the table.");
		System.out.println(
				"\tSELECT * FROM table_name WHERE <column_name> = <value>;                             Display records whose column is <value>.");
		System.out.println(
				"\tINSERT INTO <table_name> VALUES (value1, value2, ...);                              Insert a new record into the indicated table.");
		System.out.println(
				"\tDELETE FROM <table_name> WHERE row_id = key_value;                                  Delete a single row/record from a table given the row_id primary key.");
		System.out.println(
				"\tUPDATE <table_name> SET column_name = value WHERE row_id = value;                   Modifies one or more records in a table.");
		System.out.println(
				"\tVERSION;                                                                            Show the program version.");
		System.out.println(
				"\tHISTORY;                                                                            Show all recent commands.");
		System.out.println(
				"\tHELP;                                                                               Show this help information");
		System.out.println(
				"\tEXIT;                                                                               Exit the program");
		System.out.println();
		System.out.println(Utility.displayLine("*", 170));
	}
}