package fileIndexing;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utility {

	private static final String TEXT = "text";
	private static final String TINYINT = "tinyint";
	private static final String SMALLINT = "smallint";
	private static final String INT = "int";
	private static final String BIGINT = "bigint";
	private static final String REAL = "real";
	private static final String DOUBLE = "double";
	private static final String DATETIME = "datetime";
	private static final String DATE = "date";

	public static String displayLine(String string, int number) {
		String str = "";
		for (int i = 0; i < number; i++) {
			str += string;
		}
		return str;
	}

	public static String format(String string, int length) {
		return String.format("%1$" + length + "s", string);
	}

	public static String getSequenceName(String tableName, String columnName) {
		String sequenceName = tableName + "_" + columnName + "_seq";
		return sequenceName;
	}

	/********************************
	 * For Parse Query
	 *******************************/
	public static String getFilePath(String type, String filename) {
		String extention = MyDatabase.tableFormat;
		String folder = MyDatabase.userDataFolder;
		if (type.equals("master")) {
			folder = MyDatabase.systemDataFolder;
		} else if (type.equals("index")) {
			folder = MyDatabase.indicesFolder;
		} else if (type.equals("seq")) {
			folder = MyDatabase.seqFolder;
		}

		Path path = FileSystems.getDefault().getPath(MyDatabase.tableLocation, folder, filename + extention);
		return path.toString();
	}

	public static String getIndexName(String tableName, String[] columnList) {
		String indexName = "unique_" + tableName + "_";
		indexName += String.join("_", columnList) + "_idx";
		return indexName;
	}

	public static String getOSPath(String[] tokens) {
		StringBuilder str = new StringBuilder();
		for (String token : tokens) {
			str.append(token);
			str.append(File.separator);
		}
		return str.toString();
	}

	public static LinkedHashMap<String, ArrayList<String>> buildInsertRecord(List<String> values) {
		LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
		List<String> columnNames = new ArrayList<String>(Arrays.asList("rowid", "table_name", "column_name",
				"data_type", "ordinal_position", "is_nullable", "default", "is_unique", "auto_increment"));
		List<String> dataTypes = new ArrayList<String>(
				Arrays.asList(INT, TEXT, TEXT, TEXT, TINYINT, TEXT, TEXT, TEXT, TEXT));
		if (values.size() != columnNames.size())
			return null;
		for (int i = 0; i < values.size(); i++) {
			token.put(columnNames.get(i), new ArrayList<String>(Arrays.asList(dataTypes.get(i), values.get(i))));
		}
		return token;
	}

	/* * Utility method for checking if table exists in Java */
	public static boolean isTableExist(String tableName) {
		File tableFile = new File(
				MyDatabase.tableLocation + "/" + MyDatabase.userDataFolder + "/" + tableName + MyDatabase.tableFormat);
		return tableFile.exists();
	}

	public static boolean checkValue(String type, String value) {
		switch (type.toLowerCase()) {
		case TEXT:
			if ((value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
					|| (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"'))
				return true;
			break;
		case TINYINT:
			if (Integer.parseInt(value) >= Byte.MIN_VALUE && Integer.parseInt(value) <= Byte.MAX_VALUE)
				return true;
			break;
		case SMALLINT:
			if (Integer.parseInt(value) >= Short.MIN_VALUE && Integer.parseInt(value) <= Short.MAX_VALUE)
				return true;
			break;
		case INT:
			if (Integer.parseInt(value) >= Integer.MIN_VALUE && Integer.parseInt(value) <= Integer.MAX_VALUE)
				return true;
			break;
		case BIGINT:
			if (Long.parseLong(value) >= Long.MIN_VALUE && Long.parseLong(value) <= Long.MAX_VALUE)
				return true;
			break;
		case REAL:
			if (Float.parseFloat(value) >= Float.MIN_VALUE && Float.parseFloat(value) <= Float.MAX_VALUE)
				return true;
			break;
		case DOUBLE:
			if (Double.parseDouble(value) >= Double.MIN_VALUE && Double.parseDouble(value) <= Double.MAX_VALUE)
				return true;
			break;
		case DATE:
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			try {
				if (value.contains("\'")) {
					value = value.replaceAll("\'", "");
				}
				Date date = dateFormat.parse(value);
			} catch (ParseException e) {
				return false;
			}
			return true;
		case DATETIME:
			SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
			try {
				if (value.contains("\'")) {
					value = value.replaceAll("\'", "");
				}
				Date date = dateTime.parse(value);
			} catch (ParseException e) {
				return false;
			}
			return true;
		default:
			return false;
		}
		return false;
	}

}