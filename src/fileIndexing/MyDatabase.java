package fileIndexing;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.Scanner;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Arrays;

public class MyDatabase {
	protected static final String masterTableName = "master_tables";
	protected static String masterTableFilePath;
	protected static String masterColTableFilePath;
	public static final String masterColumnTableName = "master_columns";
	protected static final String tableLocation = "data";
	protected static final String userDataFolder = "user_data";
	protected static final String systemDataFolder = "catalog";
	protected static final String indicesFolder = "indices";
	protected static final String seqFolder = "sequences";
	protected static final String tableFormat = ".tbl";
	private static final String prompt = "mdbsql> ";
	protected static boolean isExit = false;
	protected static RandomAccessFile dbColumnFile;
	protected static RandomAccessFile dbMasterTableFile;
	protected static ArrayList<String> history;
	static long pageSize = 512;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	private static void buildDBColumnTable(RandomAccessFile mDBColumnFile, BTree mDBColumnFiletree) throws Exception {
		if (mDBColumnFile.length() > 0)
			return;

		LinkedHashMap<String, ArrayList<String>> token;

		token = Utility.buildInsertRecord(
				Arrays.asList("1", MyDatabase.masterTableName, "rowid", "int", "1", "no", "na", "no", "no"));
		mDBColumnFiletree.createNewTableLeaf(token);

		token = Utility.buildInsertRecord(
				Arrays.asList("2", MyDatabase.masterTableName, "table_name", "text", "2", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(
				Arrays.asList("3", MyDatabase.masterColumnTableName, "rowid", "int", "1", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(Arrays.asList("4", MyDatabase.masterColumnTableName, "table_name", "text",
				"2", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(Arrays.asList("5", MyDatabase.masterColumnTableName, "column_name", "text",
				"3", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(
				Arrays.asList("6", MyDatabase.masterColumnTableName, "data_type", "text", "4", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(Arrays.asList("7", MyDatabase.masterColumnTableName, "ordinal_position",
				"tinyint", "5", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(Arrays.asList("8", MyDatabase.masterColumnTableName, "is_nullable", "text",
				"6", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(
				Arrays.asList("9", MyDatabase.masterColumnTableName, "default", "text", "7", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);

		token = Utility.buildInsertRecord(Arrays.asList("10", MyDatabase.masterColumnTableName, "is_unique", "text",
				"8", "no", "na", "no", "no"));
		mDBColumnFiletree.insertNewRecord(token);
	}

	private static void buildDatabase(RandomAccessFile mDBtableFile, BTree mDBtabletree) throws Exception {
		if (mDBtableFile.length() > 0)
			return;

		LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
		token.put("rowid", new ArrayList<String>(Arrays.asList("int", "1")));
		token.put("table_name", new ArrayList<String>(Arrays.asList("text", MyDatabase.masterTableName)));
		mDBtabletree.createNewTableLeaf(token);

		token.clear();
		token.put("rowid", new ArrayList<String>(Arrays.asList("int", "2")));
		token.put("table_name", new ArrayList<String>(Arrays.asList("text", MyDatabase.masterColumnTableName)));
		mDBtabletree.insertNewRecord(token);
	}

	public static void main(String[] args) {
		// File system setup
		File folder = new File(tableLocation);
		if (!folder.exists()) {
			folder.mkdir();
			folder = new File(Utility.getOSPath(new String[] { tableLocation, userDataFolder }));
			folder.mkdir();
			folder = new File(Utility.getOSPath(new String[] { tableLocation, systemDataFolder }));
			folder.mkdir();
			folder = new File(Utility.getOSPath(new String[] { tableLocation, indicesFolder }));
			folder.mkdir();
			folder = new File(Utility.getOSPath(new String[] { tableLocation, seqFolder }));
			folder.mkdir();
		}

		history = new ArrayList<>();

		try {
			dbMasterTableFile = new RandomAccessFile(Utility.getFilePath("master", masterTableName), "rw");
			dbColumnFile = new RandomAccessFile(Utility.getFilePath("master", masterColumnTableName), "rw");

			BTree mDBtabletree = new BTree(dbMasterTableFile, MyDatabase.masterTableName, false, true);
			BTree mDBColumnFiletree = new BTree(dbColumnFile, MyDatabase.masterColumnTableName, true, false);

			buildDatabase(dbMasterTableFile, mDBtabletree);
			buildDBColumnTable(dbColumnFile, mDBColumnFiletree);

		} catch (Exception e) {
			System.out.println("Unexpected Error: " + e.getMessage());
		}

		// Greet user!
		WelcomePage.splashScreen();

		String userCommand = "";
		while (!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().trim().replace("\n", "").replace("\r", "").toLowerCase();
			/* Handle executing recent commands from history */
			if (userCommand.length() < 5 && userCommand.charAt(0) == '!'
					&& (Integer.valueOf(userCommand.substring(1)) > 0
							|| Integer.valueOf(userCommand.substring(1)) < 0)) {
				int index = Integer.parseInt(userCommand.substring(1));
				if (index < 0)
					index = history.size() + index;
				else
					index--;
				if (index < 0 || history.size() <= index) {
					System.out.println("No history found at that index");
					continue;
				}
				userCommand = history.get(index);

			}
			if (!userCommand.equals("history"))
				history.add(userCommand);
			ParseQuery.parse(userCommand);
		}
		System.out.println("We are exiting the database.");
	}
}
