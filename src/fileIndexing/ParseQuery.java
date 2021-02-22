package fileIndexing;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Arrays;

public class ParseQuery {

	private static final String NULL = "NULL";

	private static final String DEFAULT = "default";

	private static final String NOT_NULL = "not null";

	private static final String PRIMARY_KEY = "primary key";

	private static final String UNIQUE = "unique";

	private static final String NA = "na";

	private static final String YES = "yes";

	private static final String ERROR = "Error occured. Please check the syntax.";

	private static final String HISTORY = "history";

	private static final String QUIT = "quit";

	private static final String EXIT = "exit";

	private static final String VERSION = "version";

	private static final String HELP = "help";

	private static final String SHOW = "show";

	private static final String DELETE = "delete";

	private static final String DROP = "drop";

	private static final String UPDATE = "update";

	private static final String INSERT = "insert";

	private static final String CREATE = "create";

	private static final String SELECT = "select";

	public static void parse(String query) {
		/*
		 * query is an array of Strings that contains one token per array element The
		 * first token can be used to determine the type of command The other tokens can
		 * be used to pass relevant parameters to each command-specific method inside
		 * each case statement
		 */
		ArrayList<String> command = new ArrayList<String>(Arrays.asList(query.split(" ")));

		/*
		 * This switch handles a very small list of hardcoded commands of known syntax.
		 * You will want to rewrite this method to interpret more complex commands.
		 */
		switch (command.get(0)) {
		case SELECT:
			parseQuery(query);
			break;
		case CREATE:
			if (command.get(1).equals("table"))
				parseCreateString(query);
			else
				parseCreateIndexString(query);
			break;
		case INSERT:
			parseInsertString(query);
			break;
		case UPDATE:
			parseUpdateString(query);
			break;
		case DROP:
			if (command.get(1).equals("table"))
				QueryHandler.dropTable(command.get(2));
			else
				System.out.println(ERROR);
			break;
		case DELETE:
			parseDeleteString(query);
			break;
		case SHOW:
			parseShowString(query);
			break;
		case HELP:
			QueryHandler.help();
			break;
		case VERSION:
			WelcomePage.displayVersion();
			break;
		case EXIT:
			MyDatabase.isExit = true;
			break;
		case QUIT:
			MyDatabase.isExit = true;
			break;
		case HISTORY:
			QueryHandler.printHistory();
			break;
		default:
			System.out.println(
					"Please check the query. The query you entered does not match with any commands in database : \""
							+ query + "\"");
			break;
		}
	}

	public static void parseQuery(String query) {
		String tableName = "";
		ArrayList<String> cols = new ArrayList<String>();
		Map<String, ArrayList<String>> tableData = new LinkedHashMap<String, ArrayList<String>>();
		RandomAccessFile newTable = null;
		RandomAccessFile colFile;
		RandomAccessFile tableFile;
		try {
			query = query.replace("*", " * ");
			query = query.replace("=", " = ");
			query = query.replace(",", " , ");
			ArrayList<String> splitQuery = new ArrayList<String>(Arrays.asList(query.split("\\s+")));

			colFile = new RandomAccessFile(Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw");
			tableFile = new RandomAccessFile(Utility.getFilePath("master", MyDatabase.masterTableName), "rw");

			BTree column = new BTree(colFile, MyDatabase.masterColumnTableName, true, false);
			int flag = 1;
			if (splitQuery.get(1).equals("*") && splitQuery.get(2).equals("from") && splitQuery.size() == 4) {
				tableName = splitQuery.get(3);
				tableData = column.getSchema(splitQuery.get(3));
				if (tableData != null) {
					BTree tableBTree;
					try {
						if (tableName.trim().equals(MyDatabase.masterTableName)) {
							tableBTree = new BTree(tableFile, tableName, false, true);

						} else if (tableName.trim().equals(MyDatabase.masterColumnTableName)) {
							tableBTree = column;

						} else {
							newTable = new RandomAccessFile(Utility.getFilePath("user", tableName), "rw");
							tableBTree = new BTree(newTable, tableName);
						}
						QueryHandler.printTable(tableBTree.printAll());
						try {
							if (newTable != null)
								newTable.close();
						} catch (IOException e) {
							System.out.println("Unexpected Error");
							// e.printStackTrace();
						}

					}

					catch (FileNotFoundException e) {
						System.out.println("Table file not found");
					}
				}
			} else if (splitQuery.get(1).equals("*") && splitQuery.get(2).equals("from")
					&& splitQuery.get(4).equals("where") && splitQuery.get(6).equals("=")) {
				tableName = splitQuery.get(3);
				tableData = column.getSchema(splitQuery.get(3));
				if (tableData != null && tableData.keySet().contains(splitQuery.get(5))) {
					BTree table;
					try {

						if (tableName.trim().equals(MyDatabase.masterTableName)) {
							table = new BTree(tableFile, tableName, false, true);

						} else if (tableName.trim().equals(MyDatabase.masterColumnTableName)) {
							table = new BTree(
									new RandomAccessFile(
											Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw"),
									tableName, true, false);

						} else {
							newTable = new RandomAccessFile(Utility.getFilePath("user", tableName), "rw");
							table = new BTree(newTable, tableName);
						}

					} catch (FileNotFoundException e) {
						System.out.println("Table file not found");
						tableFile.close();
						return;
					}
					// search cond
					ArrayList<String> arryL = new ArrayList<String>();
					Integer ordinalPos = 0;
					String dataType = "";
					for (String key : tableData.keySet()) {
						if (key.equals(splitQuery.get(5))) {
							dataType = tableData.get(key).get(0);
							break;
						}
						ordinalPos++;
					}
					arryL.add(ordinalPos.toString()); // search cond col ordinal
					// position
					arryL.add(dataType); // search cond col data type
					if ((splitQuery.get(7).charAt(0) == '\''
							&& splitQuery.get(7).charAt(splitQuery.get(7).length() - 1) == '\'')
							|| (splitQuery.get(7).charAt(0) == '"'
									&& splitQuery.get(7).charAt(splitQuery.get(7).length() - 1) == '"'))
						arryL.add(splitQuery.get(7).substring(1, splitQuery.get(7).length() - 1));
					else
						arryL.add(splitQuery.get(7));
					// arryL.add(queryTokens.get(7)); // search cond col value

					if (ordinalPos == 0) {
						LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
						ArrayList<String> array = new ArrayList<String>();
						array.add(dataType);
						if ((splitQuery.get(7).charAt(0) == '\''
								&& splitQuery.get(7).charAt(splitQuery.get(7).length() - 1) == '\'')
								|| (splitQuery.get(7).charAt(0) == '"'
										&& splitQuery.get(7).charAt(splitQuery.get(7).length() - 1) == '"'))
							array.add(splitQuery.get(7).substring(1, splitQuery.get(7).length() - 1));
						else
							array.add(splitQuery.get(7));

						token.put(splitQuery.get(5), new ArrayList<String>(array));
						LinkedHashMap<String, ArrayList<String>> op = table.searchWithPrimaryKey(token);
						List<LinkedHashMap<String, ArrayList<String>>> temp = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
						temp.add(op);
						QueryHandler.printTable(temp);

					} else {
						List<LinkedHashMap<String, ArrayList<String>>> op = table.searchWithNonPK(arryL);
						QueryHandler.printTable(op);

					}
					try {
						if (newTable != null)
							newTable.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						// System.out.println("Unexpected Error");
					}

				}
			} else {
				tableName = splitQuery.get(splitQuery.indexOf("from") + 1);
				tableData = column.getSchema(splitQuery.get(splitQuery.indexOf("from") + 1));
				if (tableData != null) {
					for (int i = 1; i < splitQuery.indexOf("from") && flag == 1; i++) {
						if (tableData.keySet().contains(splitQuery.get(i)) && splitQuery.get(i + 1).equals(",")) {
							cols.add(splitQuery.get(i));
							i++;
						} else if (tableData.keySet().contains(splitQuery.get(i))
								&& splitQuery.get(i + 1).equals("from"))
							cols.add(splitQuery.get(i));
						else
							flag = 0;
					}

					if (flag == 1) // select "coln" from table;
					{
						ArrayList<String> removeColumn = new ArrayList<String>(tableData.keySet());
						removeColumn.removeAll(cols);
						BTree tableBTree;
						try {

							if (tableName.trim().equals(MyDatabase.masterTableName)) {
								tableBTree = new BTree(tableFile, tableName, false, true);

							} else if (tableName.trim().equals(MyDatabase.masterColumnTableName)) {
								tableBTree = new BTree(
										new RandomAccessFile(
												Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw"),
										tableName, true, false);

							} else {
								newTable = new RandomAccessFile(Utility.getFilePath("user", tableName), "rw");
								tableBTree = new BTree(newTable, tableName);
							}

							List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.printAll();
							for (LinkedHashMap<String, ArrayList<String>> map : op) {

								for (String x : removeColumn) {
									map.remove(x);
								}

							}
							if (!splitQuery.contains("where")) // condition not
							// working
							{
								QueryHandler.printTable(op);
							}
							if (newTable != null)
								newTable.close();
						}

						catch (Exception e) {
							System.out.println("Table file not found");
						}

					}
					if (splitQuery.size() > splitQuery.indexOf("from") + 2
							&& splitQuery.get(splitQuery.indexOf("from") + 2).equals("where")
							&& splitQuery.get(splitQuery.indexOf("from") + 4).equals("=")
							&& tableData.keySet().contains(splitQuery.get(splitQuery.indexOf("from") + 3))) {

						ArrayList<String> removeColumn = new ArrayList<String>(tableData.keySet());
						removeColumn.removeAll(cols);

						BTree tableBTree;
						try {

							if (tableName.trim().equals(MyDatabase.masterTableName)) {
								tableBTree = new BTree(tableFile, tableName, false, true);

							} else if (tableName.trim().equals(MyDatabase.masterColumnTableName)) {
								tableBTree = new BTree(
										new RandomAccessFile(
												Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw"),
										tableName, true, false);

							} else {
								newTable = new RandomAccessFile(Utility.getFilePath("user", tableName), "rw");
								tableBTree = new BTree(newTable, tableName);
							}

						} catch (FileNotFoundException e) {
							System.out.println("Table file not found");
							return;
						}
						// search cond
						ArrayList<String> arryL = new ArrayList<String>();
						Integer ordinalPos = 0;
						String dataType = "";
						for (String key : tableData.keySet()) {
							if (key.equals(splitQuery.get(splitQuery.size() - 3))) {
								dataType = tableData.get(key).get(0);
								break;
							}
							ordinalPos++;
						}
						arryL.add(ordinalPos.toString()); // search cond col
						// ordinal position
						arryL.add(dataType); // search cond col data type
						if ((splitQuery.get(splitQuery.size() - 1).charAt(0) == '\''
								&& splitQuery.get(splitQuery.size() - 1)
										.charAt(splitQuery.get(splitQuery.size() - 1).length() - 1) == '\'')
								|| (splitQuery.get(splitQuery.size() - 1).charAt(0) == '"'
										&& splitQuery.get(splitQuery.size() - 1)
												.charAt(splitQuery.get(splitQuery.size() - 1).length() - 1) == '"'))
							splitQuery.get(splitQuery.size() - 1).substring(1,
									splitQuery.get(splitQuery.size() - 1).length() - 1);
						arryL.add(splitQuery.get(splitQuery.size() - 1)); // search
						// cond
						// col
						// value

						if (ordinalPos == 0) {
							LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
							ArrayList<String> array = new ArrayList<String>();
							array.add(dataType);
							array.add(splitQuery.get(7));

							token.put(splitQuery.get(5), new ArrayList<String>(array));
							LinkedHashMap<String, ArrayList<String>> op = tableBTree.searchWithPrimaryKey(token);
							List<LinkedHashMap<String, ArrayList<String>>> temp = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
							temp.add(op);

							for (LinkedHashMap<String, ArrayList<String>> map : temp) {

								for (String x : removeColumn) {
									map.remove(x);
								}

							}
							QueryHandler.printTable(temp);

						} else {
							List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.searchWithNonPK(arryL);
							for (LinkedHashMap<String, ArrayList<String>> map : op) {

								for (String x : removeColumn) {
									map.remove(x);
								}

							}
							QueryHandler.printTable(op);

						}
						try {

							if (newTable != null)
								newTable.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							System.out.println("IO Exception: " + e.getMessage());
						}

					}

				}
			}
			if (flag == 0)
				System.out.println(ERROR);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			System.out.println(ERROR);
		}
	}

	public static void parseCreateString(String createTableString) {
		/* Define table file name */
		String[] parsedTokens = null;
		String[] token1 = null;
		String tableName = "";
		if (createTableString.contains("(") && createTableString.contains(")")) {
			parsedTokens = createTableString.split("[,()]");
			token1 = parsedTokens[0].split("[ ]");
			tableName = token1[2];
			if (tableName.length() > 15) {
				System.out.println("Table name is too long!");
				return;
			}
		} else {
			System.out.println("Syntax Error! Please enter correct query");
		}

		try {
			RandomAccessFile mDBtableFile = new RandomAccessFile(
					Utility.getFilePath("master", MyDatabase.masterTableName), "rw");
			RandomAccessFile mDBColumnFile = new RandomAccessFile(
					Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw");

			BTree mDBtabletree = new BTree(mDBtableFile, MyDatabase.masterTableName, false, true);
			BTree mDBColumntree = new BTree(mDBColumnFile, MyDatabase.masterColumnTableName, true, false);

			List<LinkedHashMap<String, ArrayList<String>>> schematableColList = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
			LinkedHashMap<String, ArrayList<String>> newTable = new LinkedHashMap<String, ArrayList<String>>();

			if (tableName.equals(MyDatabase.masterColumnTableName) || tableName.equals(MyDatabase.masterTableName)) {
				System.out.println("You have chose a reserved keyword for table name. Please change and try again!");
				return;
			}

			/* Method call to create a .tbl file */
			// System.out.println("calling isTableExist");
			if (Utility.isTableExist(tableName)) {
				System.out.println("Table \"" + tableName + "\" exists.");
				return;
			}

			if (!"row_id int primary key".equalsIgnoreCase(parsedTokens[1])) {
				System.out.println("First column must be row_id and it should be a primary key");
				return;
			}

			/* Add records to base column table */
			int dbRowId = mDBColumntree.getNextMaxRowID() + 1;

			// Check all other columns
			String supportedTypes = String.join("|", dataTypes);
			Pattern columnPattern = Pattern.compile("([a-z0-9_]+)\\s+(" + supportedTypes + ")\\s*([a-z0-9\\s]*)");
			for (int i = 1; i < parsedTokens.length; i++) {
				Matcher columnMatcher = columnPattern.matcher(parsedTokens[i]);
				if (!columnMatcher.find()) {
					System.out.println("Syntax Error. Please check the syntax in help and try again with correct query!"
							+ columnMatcher.groupCount() + " " + columnMatcher.group(1));
					return;
				}
				String columnName = columnMatcher.group(1);
				String columnType = columnMatcher.group(2);
				String constraintsText = columnMatcher.group(3);

				/**
				 * Handle unique constraint
				 */
				String isUnique = constraintsText.contains(UNIQUE) ? YES : "no";
				if (constraintsText.contains(UNIQUE)) {
					String indexName = Utility.getIndexName(tableName, new String[] { columnName });
					RandomAccessFile indexFile = new RandomAccessFile(Utility.getFilePath("index", indexName), "rw");
					indexFile.setLength(0);
					if (indexFile != null)
						indexFile.close();
				}
				String autoincrement = "no";
				if (constraintsText.contains("autoincrement")) {
					autoincrement = YES;
					int seed = 1;
					int incrementBy = 1;
					QueryHandler.createSequence(tableName, columnName, seed, incrementBy);
				}

				String isNullable = YES;
				String defaultValue = NA;
				if (constraintsText.contains(NOT_NULL) || constraintsText.contains(PRIMARY_KEY)
						|| constraintsText.contains(DEFAULT))
					isNullable = "no";
				// default constraint
				if (constraintsText.contains(DEFAULT) && constraintsText.split("\\s+").length > 1) {
					defaultValue = constraintsText.split(" ")[1];
				}
				schematableColList
						.add(Utility.buildInsertRecord(Arrays.asList(String.valueOf(dbRowId++), tableName, columnName,
								columnType, String.valueOf(i + 1), isNullable, defaultValue, isUnique, autoincrement)));
			}

			for (LinkedHashMap<String, ArrayList<String>> row : schematableColList) {
				mDBColumntree.insertNewRecord(row);
			}

			/* Add rows to base table */
			newTable.put("rowid",
					new ArrayList<String>(Arrays.asList("int", String.valueOf(mDBtabletree.getNextMaxRowID() + 1))));
			newTable.put("table_name", new ArrayList<String>(Arrays.asList("text", tableName)));
			mDBtabletree.insertNewRecord(newTable);

			/**
			 * Create required table and initialize with zero bytes
			 */
			RandomAccessFile tableFile = new RandomAccessFile(Utility.getFilePath("user", tableName), "rw");
			tableFile.setLength(0);

			new BTree(tableFile, tableName).createEmptyTable();

			if (tableFile != null) {
				tableFile.close();
			}
			System.out.println("Table was created successfully!");
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	public static void parseCreateIndexString(String command) {
		// CREATE [UNIQUE] INDEX ON table_name (column_list);
		command = command.replaceAll(" +", " ");
		Pattern topLevel = Pattern.compile("create\\s(unique\\s)?index on\\s([a-z0-9_]+)\\s(\\(([a-z0-9_,\\s]+)\\))");
		Matcher matcher = topLevel.matcher(command);
		if (!matcher.find() || matcher.groupCount() < 4) {
			System.out.println("Syntax Error. Please check and try again!");
			return;
		}

		String tableName = matcher.group(2);
		try {
			if (tableName.equals(MyDatabase.masterColumnTableName) || tableName.equals(MyDatabase.masterTableName)) {
				System.out.println("You cannot create index on master tables.");
				return;
			}

			File folder = new File(
					Utility.getOSPath(new String[] { MyDatabase.tableLocation, MyDatabase.userDataFolder }));
			File[] listOfFiles = folder.listFiles();
			boolean tableFound = false;
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].getName().equals(tableName + ".tbl")) {
					tableFound = true;
					break;
				}
			}

			if (!tableFound) {
				System.out.println(String.format("Table %s not found in database!", tableName));
				return;
			}

			String[] columnList = matcher.group(4).split(",");

			if (columnList.length < 1) {
				System.out.println("Columns should be specified");
				return;
			}

			String indexName = Utility.getIndexName(tableName, columnList);

			/**
			 * Create index file and initialize with zero bytes
			 */
			RandomAccessFile indexFile = new RandomAccessFile(Utility.getFilePath("index", indexName), "rw");
			indexFile.setLength(0);

			// new BTree(indexFile, indexName).createEmptyTable();

			if (indexFile != null) {
				indexFile.close();
			}
			System.out.println(String.format("Index %s created", indexName));
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	public static void parseInsertString(String insertTableString) {
		try {
			BTree columnBTree = new BTree(MyDatabase.dbColumnFile, MyDatabase.masterColumnTableName, true, false);
			insertTableString = insertTableString.replace("(", " ( ");
			insertTableString = insertTableString.replace(")", " ) ");
			insertTableString = insertTableString.replace(",", " , ");
			Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
			Map<String, ArrayList<String>> tableVal = new LinkedHashMap<String, ArrayList<String>>();
			ArrayList<String> colName = new ArrayList<String>();
			ArrayList<String> insertTableTokens = new ArrayList<String>(Arrays.asList(insertTableString.split("\\s+")));
			int flag = 1;
			File folder = new File(
					Utility.getOSPath(new String[] { MyDatabase.tableLocation, MyDatabase.userDataFolder }));
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].getName().equals(insertTableTokens.get(2) + ".tbl"))
					flag = 0;
			}
			if (flag == 1 || !insertTableTokens.get(1).equals("into"))
				System.out.println("Table does not exist/Syntax Error");
			else
				tableInfo = columnBTree.getSchema(insertTableTokens.get(2));
			if (insertTableTokens.get(3).equals("(")) {
				for (String x : tableInfo.keySet())
					if (tableInfo.get(x).contains("no"))
						colName.add(x);
				int k = insertTableTokens.indexOf(")");
				if (insertTableTokens.get(k + 1).equals("values") && insertTableTokens.get(k + 2).equals("(")
						&& insertTableTokens.get(insertTableTokens.size() - 1).equals(")")) {
					for (int i = 4; !insertTableTokens.get(i).equals(")") && flag == 0; i++) {
						if (colName.contains(insertTableTokens.get(i)))
							colName.remove(insertTableTokens.get(i));
						if (tableInfo.keySet().contains(insertTableTokens.get(i))
								&& (Utility.checkValue(tableInfo.get(insertTableTokens.get(i)).get(0),
										insertTableTokens.get(k + i - 1)))
								&& insertTableTokens.get(i + 1).equals(",")) {
							tableVal.put(insertTableTokens.get(i),
									new ArrayList<String>(Arrays.asList(tableInfo.get(insertTableTokens.get(i)).get(0),
											insertTableTokens.get(k + i - 1))));
							i++;
						} else if (tableInfo.keySet().contains(insertTableTokens.get(i))
								&& (Utility.checkValue(tableInfo.get(insertTableTokens.get(i)).get(0),
										insertTableTokens.get(k + i - 1)))
								&& insertTableTokens.get(i + 1).equals(")"))
							tableVal.put(insertTableTokens.get(i),
									new ArrayList<String>(Arrays.asList(tableInfo.get(insertTableTokens.get(i)).get(0),
											insertTableTokens.get(k + i - 1))));
						else
							flag = 1;
					}
				} else
					flag = 1;
				if (colName.size() != 0)
					flag = 1;
			} else if (insertTableTokens.get(3).equals("values") && insertTableTokens.get(4).equals("(")
					&& insertTableTokens.get(insertTableTokens.size() - 1).equals(")")) {
				int k = 5;
				for (String x : tableInfo.keySet()) {
					if (tableInfo.get(x).get(1).equals("no")) {
						if (Utility.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
								&& insertTableTokens.get(k + 1).equals(",")) {
							tableVal.put(x, new ArrayList<String>(
									Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
							k += 2;
						} else if (Utility.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
								&& insertTableTokens.get(k + 1).equals(")")) {
							tableVal.put(x, new ArrayList<String>(
									Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
							k++;
						} else
							flag = 1;
					} else {
						if (!insertTableTokens.get(k).equals(",") && !insertTableTokens.get(k).equals(")")) {
							if (Utility.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
									&& insertTableTokens.get(k + 1).equals(",")) {
								tableVal.put(x, new ArrayList<String>(
										Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
								k += 2;
							} else if (Utility.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
									&& insertTableTokens.get(k + 1).equals(")")) {
								tableVal.put(x, new ArrayList<String>(
										Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
								k++;
							}
						} else {
							tableVal.put(x, new ArrayList<String>(Arrays.asList(tableInfo.get(x).get(0), NULL)));
						}

					}
				}
			} else
				flag = 1;
			if (flag == 0) {
				int primaryKeyVal = -1;
				for (String key : tableVal.keySet()) {
					String primaryKey = tableVal.get(key).get(1);
					primaryKeyVal = Integer.parseInt(primaryKey);
					break;
				}
				for (String key : tableVal.keySet()) {
					tableInfo.put(key, tableVal.get(key));
				}
				// String fileName = tableLocation + insertTableTokens.get(2) +
				// tableFormat;
				try {
					RandomAccessFile newTable = new RandomAccessFile(
							Utility.getFilePath("user", insertTableTokens.get(2)), "rw");
					BTree tableTree = new BTree(newTable, insertTableTokens.get(2));

					// if sequence related columns are null, add values
					String tableName = insertTableTokens.get(2);
					for (String col : tableInfo.keySet()) {
						String colValue = tableInfo.get(col).get(1);
						// handle only when col value is not sent
						if (!colValue.equals(NULL))
							continue;
						int seqValue = QueryHandler.nextValueInSequence(tableName, col);
						// ignore if there is no seq file
						if (seqValue == 0)
							continue;
						tableInfo.put(col, new ArrayList<String>(
								Arrays.asList(tableInfo.get(col).get(0), String.valueOf(seqValue))));
						QueryHandler.update(tableName, col);
					}

					if (tableTree.isEmptyTable()) {
						tableTree.createNewTableLeaf(tableInfo);

					} else {
						if (tableTree.isPKPresent(primaryKeyVal)) {
							System.out.println(" Primary key with value " + primaryKeyVal + " already exists");
							return;
						} else {
							for (String x : tableInfo.keySet())
								if ((tableInfo.get(x).get(1).charAt(0) == '\''
										&& tableInfo.get(x).get(1).charAt(tableInfo.get(x).get(1).length() - 1) == '\'')
										|| (tableInfo.get(x).get(1).charAt(0) == '"' && tableInfo.get(x).get(1)
												.charAt(tableInfo.get(x).get(1).length() - 1) == '"'))
									tableInfo.put(x, new ArrayList<String>(Arrays.asList("text", tableInfo.get(x).get(1)
											.substring(1, tableInfo.get(x).get(1).length() - 1))));
							tableTree.insertNewRecord(tableInfo);
							if (newTable != null)
								newTable.close();

						}
					}

				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("Unexpected Error");
				}

				System.out.println("1 row inserted");

			} else
				System.out.println("Error in syntax");
		} catch (Exception e) {
			System.out.println(ERROR);
		}
	}

	public static void parseShowString(String showTableString) {
		try {
			ArrayList<String> showTableTokens = new ArrayList<String>(Arrays.asList(showTableString.split("\\s+")));
			if (showTableTokens.get(1).equals("tables"))
				QueryHandler.showTables();
			else
				System.out.println(ERROR);
		} catch (Exception e) {
			System.out.println(ERROR);
		}
	}

	public static void parseUpdateString(String updateTableString) {
		try {
			updateTableString = updateTableString.replace("=", " = ");
			BTree columnBTree = new BTree(MyDatabase.dbColumnFile, MyDatabase.masterColumnTableName, true, false);
			ArrayList<String> updateTableTokens = new ArrayList<String>(Arrays.asList(updateTableString.split("\\s+")));
			Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
			int flag = 1;
			if (updateTableTokens.get(2).equals("set") && updateTableTokens.get(4).equals("=")) {
				if (updateTableTokens.size() > 6 && updateTableTokens.get(6).equals("where")
						&& updateTableTokens.get(8).equals("=")) {
					tableInfo = columnBTree.getSchema(updateTableTokens.get(1));
					if (tableInfo != null) {
						if (tableInfo.keySet().contains(updateTableTokens.get(3))
								&& tableInfo.keySet().contains(updateTableTokens.get(7))) {
							if (Utility.checkValue(tableInfo.get(updateTableTokens.get(3)).get(0),
									updateTableTokens.get(5))
									&& Utility.checkValue(tableInfo.get(updateTableTokens.get(7)).get(0),
											updateTableTokens.get(9))) {

								ArrayList array = new ArrayList<String>();
								LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();

								BTree tableTree = null;
								RandomAccessFile newTable = null;
								try {
									newTable = new RandomAccessFile(
											Utility.getFilePath("user", updateTableTokens.get(1)), "rw");
									tableTree = new BTree(newTable, updateTableTokens.get(1));
								} catch (FileNotFoundException e1) {
									System.out.println(" Table not found during update");
									return;
								}
								String dataType = tableInfo.get(updateTableTokens.get(7)).get(0);
								array.add(dataType);
								if ((updateTableTokens.get(9).charAt(0) == '\'' && updateTableTokens.get(9)
										.charAt(updateTableTokens.get(9).length() - 1) == '\'')
										|| (updateTableTokens.get(9).charAt(0) == '"' && updateTableTokens.get(9)
												.charAt(updateTableTokens.get(9).length() - 1) == '"'))
									array.add(updateTableTokens.get(9).substring(1,
											updateTableTokens.get(9).length() - 1));
								else
									array.add(new String(updateTableTokens.get(9)));

								token.put(updateTableTokens.get(7), new ArrayList<String>(array));

								LinkedHashMap<String, ArrayList<String>> result = tableTree.searchWithPrimaryKey(token);
								if (result == null) {
									return;
								}
								LinkedHashMap<String, ArrayList<String>> table = columnBTree
										.getSchema(updateTableTokens.get(1));
								for (String column : result.keySet()) {
									if (column.equals(updateTableTokens.get(3)))// colname
									{
										ArrayList<String> value = table.get(column);
										value.remove(value.size() - 1);
										if ((updateTableTokens.get(5).charAt(0) == '\'' && updateTableTokens.get(5)
												.charAt(updateTableTokens.get(5).length() - 1) == '\'')
												|| (updateTableTokens.get(5).charAt(0) == '"' && updateTableTokens
														.get(5).charAt(updateTableTokens.get(5).length() - 1) == '"'))
											value.add(updateTableTokens.get(5).substring(1,
													updateTableTokens.get(5).length() - 1)); // newValue
										else
											value.add(updateTableTokens.get(5));
										table.put(column, value);
									} else {
										ArrayList<String> value = table.get(column);
										ArrayList<String> res = result.get(column);
										value.remove(value.size() - 1);
										String val = res.get(0);
										value.add(val);
										table.put(column, value);
									}

								}
								token.clear();
								array.clear();
								dataType = tableInfo.get(updateTableTokens.get(7)).get(0);
								array.add(dataType);
								array.add(new String(updateTableTokens.get(9)));
								token.put(updateTableTokens.get(7), new ArrayList<String>(array));
								tableTree.deleteRecord(token);
								try {
									tableTree.insertNewRecord(table);
									System.out.println(" 1 row updated");
									if (newTable != null)
										newTable.close();
								} catch (Exception e) {

									System.out.println(" Update Failed");
								}

							} else
								flag = 0;
						} else
							flag = 0;
					} else
						flag = 0;
				} else {
					tableInfo = columnBTree.getSchema(updateTableTokens.get(1));
					if (tableInfo != null) {
						if (tableInfo.keySet().contains(updateTableTokens.get(3))) {
							if (Utility.checkValue(tableInfo.get(updateTableTokens.get(3)).get(0),
									updateTableTokens.get(5))) {

								int noOfRows = 0;
								BTree tableTree = null;
								RandomAccessFile newTable = null;
								try {
									newTable = new RandomAccessFile(
											Utility.getFilePath("user", updateTableTokens.get(1)), "rw");
									tableTree = new BTree(newTable, updateTableTokens.get(1));
								} catch (FileNotFoundException e1) {
									System.out.println(" Table not found during update");
									return;
								}

								List<LinkedHashMap<String, ArrayList<String>>> list_result = tableTree.printAll();
								for (LinkedHashMap<String, ArrayList<String>> result : list_result) {
									LinkedHashMap<String, ArrayList<String>> table = columnBTree
											.getSchema(updateTableTokens.get(1));
									LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
									ArrayList<String> array = new ArrayList<String>();

									for (String column : result.keySet()) {
										if (column.equals(updateTableTokens.get(3)))// colname
										{
											ArrayList<String> value = table.get(column);
											value.remove(value.size() - 1);
											if ((updateTableTokens.get(5).charAt(0) == '\'' && updateTableTokens.get(5)
													.charAt(updateTableTokens.get(5).length() - 1) == '\'')
													|| (updateTableTokens.get(5).charAt(0) == '"'
															&& updateTableTokens.get(5).charAt(
																	updateTableTokens.get(5).length() - 1) == '"'))
												value.add(updateTableTokens.get(5).substring(1,
														updateTableTokens.get(5).length() - 1)); // newValue
											else
												value.add(updateTableTokens.get(5));
											table.put(column, value);
										} else {
											ArrayList<String> value = table.get(column);
											ArrayList<String> res = result.get(column);
											value.remove(value.size() - 1);
											String val = res.get(0);
											value.add(val);
											table.put(column, value);
										}

									}
									token.clear();
									array.clear();

									array.add("int");
									array.add(new String(table.get(tableTree.getPrimaryKey()).get(1)));
									token.put(tableTree.getPrimaryKey(), new ArrayList<String>(array));
									tableTree.deleteRecord(token);
									try {
										tableTree.insertNewRecord(table);
										noOfRows++;
									} catch (Exception e) {

										System.out.println(" Update Failed");
									}

								}
								if (newTable != null)
									try {
										newTable.close();
									} catch (IOException e) {
										// TODO Auto-generated catch block
										System.out.println("Unexpected Error");
									}

								System.out.println(noOfRows + " row(s) updated.");

							} else
								flag = 0;
						} else
							flag = 0;
					} else
						flag = 0;
				}
			} else
				flag = 0;
			if (flag == 0)
				System.out.println(ERROR);
		} catch (Exception e) {
			System.out.println(ERROR);
		}
	}

	public static void parseDeleteString(String deleteTableString) {
		try {
			BTree columnBTree = new BTree(MyDatabase.dbColumnFile, MyDatabase.masterColumnTableName, true, false);
			deleteTableString = deleteTableString.replace("=", " = ");
			ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(deleteTableString.split("\\s+")));
			Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
			int flag = 1;
			if (deleteTableTokens.get(1).equals("from")) {
				if (deleteTableTokens.size() > 3 && deleteTableTokens.get(3).equals("where")
						&& deleteTableTokens.get(5).equals("=")) {
					tableInfo = columnBTree.getSchema(deleteTableTokens.get(2));
					if (tableInfo != null) {
						for (String x : tableInfo.keySet()) {
							if (!deleteTableTokens.get(4).equals(x))
								flag = 0;
							break;
						}
						String dataTypeOfDeleteKey = "int";
						if (tableInfo != null) {
							for (String x : tableInfo.keySet()) {
								if (deleteTableTokens.get(4).equals(x)) {
									dataTypeOfDeleteKey = tableInfo.get(x).get(0);
								}

								break;
							}
						}

						if (flag == 1) {
							LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
							ArrayList<String> array = new ArrayList<String>();
							array.add(dataTypeOfDeleteKey);
							if ((deleteTableTokens.get(6).charAt(0) == '\''
									&& deleteTableTokens.get(6).charAt(deleteTableTokens.get(6).length() - 1) == '\'')
									|| (deleteTableTokens.get(6).charAt(0) == '"' && deleteTableTokens.get(6)
											.charAt(deleteTableTokens.get(6).length() - 1) == '"'))
								array.add(deleteTableTokens.get(6).substring(1, deleteTableTokens.get(6).length() - 1));
							else
								array.add(deleteTableTokens.get(6));
							token.put(deleteTableTokens.get(4), new ArrayList<String>(array));
							RandomAccessFile filename;

							try {
								filename = new RandomAccessFile(Utility.getFilePath("user", deleteTableTokens.get(2)),
										"rw");

								BTree mDBColumnFiletree = new BTree(filename, deleteTableTokens.get(2));
								mDBColumnFiletree.deleteRecord(token);
								System.out.println("1 row deleted");

								if (filename != null)
									filename.close();
							} catch (Exception e) {
								// TODO Auto-generated catch block
								System.out.println("Table does not exists");
							}
						}
					} else
						System.out.println("Table does not exist!!!");
				} else {
					tableInfo = columnBTree.getSchema(deleteTableTokens.get(2));
					if (tableInfo != null)
						System.out.println("Schema doesn't exists");
					else
						System.out.println("Table does not exist!!!");
				}
			}
		} catch (Exception e) {
			System.out.println(ERROR);
		}
	}

	public static List<String> contraints = new ArrayList<String>(
			Arrays.asList(NOT_NULL, UNIQUE, PRIMARY_KEY, "autoincrement", DEFAULT));

	// Supported data types
	public static List<String> dataTypes = new ArrayList<String>(
			Arrays.asList("tinyint", "smallint", "int", "bigint", "real", "double", "datetime", "date", "text"));
}
