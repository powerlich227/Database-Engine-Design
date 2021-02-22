package fileIndexing;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class BTree {

	private static final String TEXT = "text";
	private static final String DATE2 = "date";
	private static final String DATETIME = "datetime";
	private long NO_OF_CELLS_HEADER = 0;
	private long START_OF_CELL_HEADER = 0;
	private static final String DOUBLE = "double";
	private static final String REAL = "real";
	public static final int LEAF = 13;
	private static final String BIGINT = "bigint";
	public final int NODE_TYPE_OFFSET = 1;
	private static final String INT = "int";
	private static final String SMALLINT = "smallint";
	private static final String TINYINT = "tinyint";
	private boolean isTableSchema;

	private String tableName;

	private boolean isLeafPage = false;

	private int lastPage = 1;

	public static final int INTERNAL = 5;

	private RandomAccessFile fileBinry;

	private static final int pageSize = 512;

	private int currentPage = 1;

	private long pageHeader_Offset_rightPagePointer = 0;
	private long pageHeader_array_offset = 0;
	private long pageHeader_offset = 0;

	private ZoneId zoneId = ZoneId.of("America/Chicago");

	private ArrayList<Integer> routeOfLeafPage = new ArrayList<>();

	private boolean isColumnSchema;

	private String tableKey = "rowid";

	private BTree mDBColumnFiletree;

	public BTree(RandomAccessFile file, String tableName) {
		fileBinry = file;
		this.tableName = tableName;
		try {
			if (file.length() > 0) {
				lastPage = (int) (file.length() / 512);
				currentPage = lastPage;
			}

			if (!tableName.equals(MyDatabase.masterColumnTableName) && !tableName.equals(MyDatabase.masterTableName)) {
				mDBColumnFiletree = new BTree(
						new RandomAccessFile(Utility.getFilePath("master", MyDatabase.masterColumnTableName), "rw"),
						MyDatabase.masterColumnTableName, true, false);

				for (String key : mDBColumnFiletree.getSchema(tableName).keySet()) {
					tableKey = key;
					break;
				}
			}
		} catch (Exception e) {
			System.out.println("Unexpected Error");
		}
	}

	public BTree(RandomAccessFile file, String tableName, boolean isColSchema, boolean isTableSchema) {
		this(file, tableName);
		this.isColumnSchema = isColSchema;
		this.isTableSchema = isTableSchema;
	}

	public void createNewInterior(int pageNumber, int rowID, int pageRight) {
		try {
			fileBinry.seek(0);
			fileBinry.write(5);
			fileBinry.write(1);
			fileBinry.writeShort(pageSize - 8);
			fileBinry.writeInt(pageRight);
			fileBinry.writeShort(pageSize - 8);
			fileBinry.seek(pageSize - 8);
			fileBinry.writeInt(pageNumber);
			fileBinry.writeInt(rowID);

		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}
	}

	public void deleteCellInterior(int pageLocation, int pageNumber) {
		try {
			fileBinry.seek(pageLocation * pageSize - pageSize + 1);
			short No_OfCells = fileBinry.readByte();
			int pos = No_OfCells;
			for (int i = 0; i < No_OfCells; i++) {
				fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * (i)));
				fileBinry.seek(fileBinry.readUnsignedShort());
				if (pageNumber == fileBinry.readInt()) {
					pos = i;
					fileBinry.seek(pageLocation * pageSize - pageSize + 1);
					fileBinry.write(No_OfCells - 1);
					break;
				}
			}
			int temp;
			while (pos < No_OfCells) {
				fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * (pos + 1)));
				temp = fileBinry.readUnsignedShort();
				fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * (pos)));
				fileBinry.writeShort(temp);
				pos++;
			}
			temp = 0;
			for (int i = 0; i < No_OfCells - 1; i++) {
				fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * (i)));
				if (temp < fileBinry.readUnsignedShort()) {
					fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * (i)));
					temp = fileBinry.readUnsignedShort();
				}
			}
			fileBinry.seek(pageLocation * pageSize - pageSize + 2);
			fileBinry.writeShort(temp);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public void createNewTableLeaf(Map<String, ArrayList<String>> token) {
		try {
			currentPage = 1;
			fileBinry.setLength(0);

			fileBinry.setLength(pageSize);

			writePageHeader(currentPage, true, 0, -1);

			long no_of_Bytes = payloadSizeInBytes(token);

			long cellStartOffset = (currentPage * (pageSize)) - (no_of_Bytes + 6);

			writeCell(currentPage, token, cellStartOffset, no_of_Bytes);

		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}
	}

	public void createEmptyTable() {

		try {
			currentPage = 1;
			fileBinry.setLength(0);

			fileBinry.setLength(pageSize);

			writePageHeader(currentPage, true, 0, -1);

		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}

	}

	public LinkedHashMap<String, ArrayList<String>> getSchema(String tableName) {
		ArrayList<String> vall = new ArrayList<String>();
		vall.add("1");
		vall.add("TEXT");
		vall.add(tableName);
		List<LinkedHashMap<String, ArrayList<String>>> output = searchWithNonPK(vall);

		LinkedHashMap<String, ArrayList<String>> finalResult = new LinkedHashMap<String, ArrayList<String>>();

		for (LinkedHashMap<String, ArrayList<String>> map : output) {
			ArrayList<String> val = map.get("column_name");
			ArrayList<String> defaultText = map.get("default");
			ArrayList<String> nullStringList = map.get("is_nullable");
			ArrayList<String> dataTypeList = map.get("data_type");
			ArrayList<String> value = new ArrayList<String>();

			String key = val.get(0);
			String dataType = dataTypeList.get(0);

			boolean isNullable = nullStringList.get(0).equals("yes");
			String nullString = isNullable ? "NULL" : "no";
			boolean hasDefault = defaultText != null && !defaultText.get(0).equalsIgnoreCase("na");

			value.add(dataType);

			if (hasDefault) {
				value.add(defaultText.get(0));
			} else {
				value.add(nullString);
			}

			finalResult.put(key, value);
		}

		return finalResult;

	}

	public int getNextMaxRowID() {
		currentPage = 1;
		searchRMLNode();
		readPageHeader(currentPage);
		try {
			fileBinry.seek(NO_OF_CELLS_HEADER);
			int noOfCells = fileBinry.readUnsignedByte();
			fileBinry.seek(pageHeader_array_offset + (2 * (noOfCells - 1)));
			long address = fileBinry.readUnsignedShort();
			fileBinry.seek(address);
			fileBinry.readShort();
			return fileBinry.readInt();

		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}
		return -1;

	}

	public boolean isPKPresent(int key) {

		currentPage = 1;

		int rowId = key;
		searchLeafPage(rowId, false);
		readPageHeader(currentPage);
		long[] result = getCellOffset(rowId);
		long cellOffset = result[1];
		if (cellOffset > 0) {
			try {
				fileBinry.seek(cellOffset);
				fileBinry.readUnsignedShort();
				int actualRowID = fileBinry.readInt();
				// System.out.println("Row id is " + actualRowID);
				if (actualRowID == rowId) {

					return true;
				}

			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}

		} else {
			return false;
		}

		return false;
	}

	public LinkedHashMap<String, ArrayList<String>> searchWithPrimaryKey(
			LinkedHashMap<String, ArrayList<String>> token) {

		currentPage = 1;

		LinkedHashMap<String, ArrayList<String>> value = null;
		int rowId = Integer.parseInt(token.get(tableKey).get(1));
		searchLeafPage(rowId, false);
		readPageHeader(currentPage);
		long[] result = getCellOffset(rowId);
		long cellOffset = result[1];
		if (cellOffset > 0) {
			try {
				fileBinry.seek(cellOffset);
				fileBinry.readUnsignedShort();
				int actualRowID = fileBinry.readInt();
				if (actualRowID == rowId) {

					if (isColumnSchema) {
						token = new LinkedHashMap<String, ArrayList<String>>();
						token.put("rowid", null);
						token.put("table_name", null);
						token.put("column_name", null);
						token.put("data_type", null);
						token.put("ordinal_position", null);
						token.put("is_nullable", null);
						token.put("default", null);
						token.put("is_unique", null);
						token.put("auto_increment", null);
					} else if (isTableSchema) {
						token = new LinkedHashMap<String, ArrayList<String>>();
						token.put("rowid", null);
						token.put("table_name", null);

					} else {
						token = mDBColumnFiletree.getSchema(tableName);
					}

					value = populateData(cellOffset, token);

				}

			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}

			return value;

		} else {
			System.out.println(" No rows matches");
			return null;
		}

	}

	public boolean deleteRecord(LinkedHashMap<String, ArrayList<String>> token) {
		currentPage = 1;
		boolean isDone = false;

		int rowId = Integer.parseInt(token.get(tableKey).get(1));
		searchLeafPage(rowId, false);
		readPageHeader(currentPage);
		long[] retVal = getCellOffset(rowId);
		long cellOffset = retVal[1];
		if (cellOffset > 0) {
			try {
				fileBinry.seek(cellOffset);
				fileBinry.readUnsignedShort();
				int actualRowID = fileBinry.readInt();
				// System.out.println("Row id is " + actualRowID);
				if (actualRowID == rowId) {

					fileBinry.seek(START_OF_CELL_HEADER);
					long startOfCell = fileBinry.readUnsignedShort();
					if (cellOffset == startOfCell) {

						fileBinry.seek(cellOffset);
						int payLoadSize = fileBinry.readUnsignedShort();
						fileBinry.seek(START_OF_CELL_HEADER);
						fileBinry.writeShort((int) (startOfCell - payLoadSize - 6));

					}

					fileBinry.seek(NO_OF_CELLS_HEADER);

					int No_OfCells = fileBinry.readUnsignedByte();

					int temp;
					long pos = retVal[0];
					while (pos < No_OfCells) {
						fileBinry.seek((currentPage * pageSize - pageSize + 8) + (2 * (pos + 1)));
						temp = fileBinry.readUnsignedShort();
						fileBinry.seek((currentPage * pageSize - pageSize + 8) + (2 * (pos)));
						fileBinry.writeShort(temp);
						pos++;
					}

					fileBinry.seek(NO_OF_CELLS_HEADER);
					int col = fileBinry.readUnsignedByte();
					fileBinry.seek(NO_OF_CELLS_HEADER);
					fileBinry.writeByte(--col);
					if (col == 0) {

						fileBinry.seek(START_OF_CELL_HEADER);
						fileBinry.writeShort((int) (currentPage * pageSize));

					}
					isDone = true;
				} else {

					System.out.println("No row matches");
				}

			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}

		} else {
			System.out.println(" No rows matches");
		}
		return isDone;
	}

	private void writeToInterior(int pageLocation, int pageNumber, int rowID, int pageRight) {
		try {

			fileBinry.seek(pageLocation * pageSize - pageSize + 1);
			short No_OfCells = fileBinry.readByte();
			if (No_OfCells < 49) {
				fileBinry.seek(pageLocation * pageSize - pageSize + 4);
				if (fileBinry.readInt() == pageNumber && pageRight != -1) {
					fileBinry.seek(pageLocation * pageSize - pageSize + 4);
					fileBinry.writeInt(pageRight);
					long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
					fileBinry.seek(pageLocation * pageSize - pageSize + 2);
					fileBinry.writeShort((int) cellStartOffset);
					fileBinry.seek(pageLocation * pageSize - pageSize + 1);
					fileBinry.write(No_OfCells + 1);
					fileBinry.seek(cellStartOffset);
					fileBinry.writeInt(pageNumber);
					fileBinry.writeInt(rowID);
					fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * No_OfCells));
					fileBinry.writeShort((short) cellStartOffset);
				} else {
					int flag = 0;
					for (int i = 0; i < No_OfCells; i++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						fileBinry.seek(fileBinry.readUnsignedShort());
						if (fileBinry.readInt() == pageNumber) {
							flag = 1;
							int tempRowID = fileBinry.readInt();
							fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
							fileBinry.seek(fileBinry.readUnsignedShort() + 4);
							fileBinry.writeInt(rowID);
							long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
							fileBinry.seek(pageLocation * pageSize - pageSize + 2);
							fileBinry.writeShort((int) cellStartOffset);
							fileBinry.seek(pageLocation * pageSize - pageSize + 1);
							fileBinry.write(No_OfCells + 1);
							fileBinry.seek(cellStartOffset);
							fileBinry.writeInt(pageRight);
							fileBinry.writeInt(tempRowID);
							fileBinry.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
							fileBinry.writeShort((short) cellStartOffset);
						}
					}
					if (flag == 0) {
						long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
						fileBinry.seek(pageLocation * pageSize - pageSize + 2);
						fileBinry.writeShort((int) cellStartOffset);
						fileBinry.seek(pageLocation * pageSize - pageSize + 1);
						fileBinry.write(No_OfCells + 1);
						fileBinry.seek(cellStartOffset);
						fileBinry.writeInt(pageNumber);
						fileBinry.writeInt(rowID);
						fileBinry.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
						fileBinry.writeShort((short) cellStartOffset);
					}
				}
				int tempAddi, tempAddj, tempi, tempj;
				for (int i = 0; i <= No_OfCells; i++)
					for (int j = i + 1; j <= No_OfCells; j++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						tempAddi = fileBinry.readUnsignedShort();
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
						tempAddj = fileBinry.readUnsignedShort();
						fileBinry.seek(tempAddi + 4);
						tempi = fileBinry.readInt();
						fileBinry.seek(tempAddj + 4);
						tempj = fileBinry.readInt();
						if (tempi > tempj) {
							fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
							fileBinry.writeShort(tempAddj);
							fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
							fileBinry.writeShort(tempAddi);

						}
					}
			} else {
				fileBinry.seek(pageLocation * pageSize - pageSize + 4);
				if (fileBinry.readInt() == pageNumber && pageRight != -1) {
					fileBinry.seek(pageLocation * pageSize - pageSize + 4);
					fileBinry.writeInt(pageRight);
					long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
					fileBinry.seek(pageLocation * pageSize - pageSize + 2);
					fileBinry.writeShort((int) cellStartOffset);
					fileBinry.seek(pageLocation * pageSize - pageSize + 1);
					fileBinry.write(No_OfCells + 1);
					fileBinry.seek(cellStartOffset);
					fileBinry.writeInt(pageNumber);
					fileBinry.writeInt(rowID);
					fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * No_OfCells));
					fileBinry.writeShort((short) cellStartOffset);
				} else {
					int flag = 0;
					for (int i = 0; i < No_OfCells; i++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						fileBinry.seek(fileBinry.readUnsignedShort());
						if (fileBinry.readInt() == pageNumber) {
							flag = 1;
							int tempRowID = fileBinry.readInt();
							fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
							fileBinry.seek(fileBinry.readUnsignedShort() + 4);
							fileBinry.writeInt(rowID);
							long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
							fileBinry.seek(pageLocation * pageSize - pageSize + 2);
							fileBinry.writeShort((int) cellStartOffset);
							fileBinry.seek(pageLocation * pageSize - pageSize + 1);
							fileBinry.write(No_OfCells + 1);
							fileBinry.seek(cellStartOffset);
							fileBinry.writeInt(pageRight);
							fileBinry.writeInt(tempRowID);
							fileBinry.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
							fileBinry.writeShort((short) cellStartOffset);
						}
					}
					if (flag == 0) {
						long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
						fileBinry.seek(pageLocation * pageSize - pageSize + 2);
						fileBinry.writeShort((int) cellStartOffset);
						fileBinry.seek(pageLocation * pageSize - pageSize + 1);
						fileBinry.write(No_OfCells + 1);
						fileBinry.seek(cellStartOffset);
						fileBinry.writeInt(pageNumber);
						fileBinry.writeInt(rowID);
						fileBinry.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
						fileBinry.writeShort((short) cellStartOffset);
					}
				}
				int tempAddi, tempAddj, tempi, tempj;
				for (int i = 0; i <= No_OfCells; i++)
					for (int j = i + 1; j <= No_OfCells; j++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						tempAddi = fileBinry.readUnsignedShort();
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
						tempAddj = fileBinry.readUnsignedShort();
						fileBinry.seek(tempAddi + 4);
						tempi = fileBinry.readInt();
						fileBinry.seek(tempAddj + 4);
						tempj = fileBinry.readInt();
						if (tempi > tempj) {
							fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
							fileBinry.writeShort(tempAddj);
							fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
							fileBinry.writeShort(tempAddi);

						}
					}
				if (pageLocation == 1) {
					int x, y;
					fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * 25));
					fileBinry.seek(fileBinry.readUnsignedShort());
					x = fileBinry.readInt();
					y = fileBinry.readInt();
					writePageHeader(lastPage + 1, false, 0, x);
					for (int i = 0; i < 25; i++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						fileBinry.seek(fileBinry.readUnsignedShort());
						writeToInterior(lastPage + 1, fileBinry.readInt(), fileBinry.readInt(), -1);
					}
					fileBinry.seek(pageLocation * pageSize - pageSize + 4);
					writePageHeader(lastPage + 2, false, 0, fileBinry.readInt());
					for (int i = 26; i < 50; i++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						fileBinry.seek(fileBinry.readUnsignedShort());
						writeToInterior(lastPage + 2, fileBinry.readInt(), fileBinry.readInt(), -1);
					}
					writePageHeader(1, false, 0, lastPage + 2);

					writeToInterior(1, lastPage + 1, y, lastPage + 2);
					lastPage += 2;

				} else {

					int x, y;
					fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * 25));
					fileBinry.seek(fileBinry.readUnsignedShort());
					x = fileBinry.readInt();
					y = fileBinry.readInt();
					fileBinry.seek(pageLocation * pageSize - pageSize + 4);
					writePageHeader(lastPage + 1, false, 0, fileBinry.readInt());
					fileBinry.seek(pageLocation * pageSize - pageSize + 4);
					fileBinry.writeInt(x);
					for (int i = 26; i < 50; i++) {
						fileBinry.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
						fileBinry.seek(fileBinry.readUnsignedShort());
						writeToInterior(lastPage + 1, fileBinry.readInt(), fileBinry.readInt(), -1);

					}

					fileBinry.seek(pageLocation * pageSize - pageSize + 1);
					fileBinry.write(25);

					int lastInteriorPage = routeOfLeafPage.remove(routeOfLeafPage.size() - 1);

					writeToInterior(lastInteriorPage, pageLocation, y, lastPage + 1);
					lastPage++;

				}
			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private void searchLeftMostLeafNode() {
		routeOfLeafPage.add(currentPage);
		readPageHeader(currentPage);
		if (isLeafPage) {
			routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
			return;
		} else {
			try {
				fileBinry.seek(NO_OF_CELLS_HEADER);

				int noOfColumns = fileBinry.readUnsignedByte();

				fileBinry.seek(pageHeader_array_offset);
				int address;
				if (noOfColumns > 0) {

					address = fileBinry.readUnsignedShort();

					fileBinry.seek(address);
					int pageNumber = fileBinry.readInt();

					currentPage = pageNumber;
					searchLeftMostLeafNode();

				}
			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}
		}

	}

	private void searchRMLNode() {

		routeOfLeafPage.add(currentPage);
		readPageHeader(currentPage);
		if (isLeafPage) {

			routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
			return;
		} else {
			try {
				fileBinry.seek(pageHeader_Offset_rightPagePointer);

				currentPage = fileBinry.readInt();

				searchRMLNode();

			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}
		}

	}

	public List<LinkedHashMap<String, ArrayList<String>>> printAll() {
		currentPage = 1;
		List<LinkedHashMap<String, ArrayList<String>>> result = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
		searchLeftMostLeafNode();
		while (currentPage > 0) {
			try {
				readPageHeader(currentPage);
				printCurrentPg(result);

				fileBinry.seek(pageHeader_Offset_rightPagePointer);

				currentPage = fileBinry.readInt();

			} catch (Exception e) {
				System.out.println("Unexpected Error");
			}
		}
		return result;

	}

	public List<LinkedHashMap<String, ArrayList<String>>> searchWithNonPK(ArrayList<String> value) {
		currentPage = 1;
		List<LinkedHashMap<String, ArrayList<String>>> result = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
		searchLeftMostLeafNode();
		while (currentPage > 0) {
			try {
				readPageHeader(currentPage);
				searchCurrentPg(value, result);
				// printRecordsInTheCurrentPage(result);

				fileBinry.seek(pageHeader_Offset_rightPagePointer);

				currentPage = fileBinry.readInt();

			} catch (Exception e) {
				System.out.println("Unexpected Error");
			}
		}

		return result;

	}

	private void printCurrentPg(List<LinkedHashMap<String, ArrayList<String>>> result) throws Exception {
		fileBinry.seek(NO_OF_CELLS_HEADER);
		int noOfCol = fileBinry.readUnsignedByte();

		fileBinry.seek(pageHeader_array_offset);
		long point = fileBinry.getFilePointer();
		int address = fileBinry.readUnsignedShort();

		for (int i = 0; i < noOfCol; i++) {

			fileBinry.seek(address);

			fileBinry.readUnsignedShort();
			int currentRowID = fileBinry.readInt();

			LinkedHashMap<String, ArrayList<String>> token = null;
			if (isColumnSchema) {
				token = new LinkedHashMap<String, ArrayList<String>>();
				token.put("rowid", null);
				token.put("table_name", null);
				token.put("column_name", null);
				token.put("data_type", null);
				token.put("ordinal_position", null);
				token.put("is_nullable", null);
				token.put("default", null);
				token.put("is_unique", null);
				token.put("auto_increment", null);
			} else if (isTableSchema) {
				token = new LinkedHashMap<String, ArrayList<String>>();
				token.put("rowid", null);
				token.put("table_name", null);

			} else {
				token = mDBColumnFiletree.getSchema(tableName);
			}

			result.add(populateData(address, token));

			point = (point + 2);
			fileBinry.seek(point);
			address = fileBinry.readUnsignedShort();

		}

	}

	private void searchCurrentPg(ArrayList<String> searchCond, List<LinkedHashMap<String, ArrayList<String>>> result)
			throws Exception {
		fileBinry.seek(NO_OF_CELLS_HEADER);
		int noOfCol = fileBinry.readUnsignedByte();

		fileBinry.seek(pageHeader_array_offset);
		long point = fileBinry.getFilePointer();
		int address = fileBinry.readUnsignedShort();

		for (int i = 0; i < noOfCol; i++) {

			fileBinry.seek(address);

			fileBinry.readUnsignedShort();
			int currentRowID = fileBinry.readInt();
			LinkedHashMap<String, ArrayList<String>> token = null;
			if (isColumnSchema) {
				token = new LinkedHashMap<String, ArrayList<String>>();
				token.put("rowid", null);
				token.put("table_name", null);
				token.put("column_name", null);
				token.put("data_type", null);
				token.put("ordinal_position", null);
				token.put("is_nullable", null);
				token.put("default", null);
				token.put("is_unique", null);
				token.put("auto_increment", null);
			} else if (isTableSchema) {
				token = new LinkedHashMap<String, ArrayList<String>>();
				token.put("rowid", null);
				token.put("table_name", null);

			} else {
				token = mDBColumnFiletree.getSchema(tableName);
			}
			token = populateResult(searchCond, address, token);
			if (token != null)
				result.add(token);

			point = (point + 2);
			fileBinry.seek(point);
			address = fileBinry.readUnsignedShort();

		}

	}

	private long[] getCellOffset(int rowId) {
		long[] retVal = new long[2];
		int cellOffset = -1;
		try {
			fileBinry.seek(NO_OF_CELLS_HEADER);

			int noOfColumns = fileBinry.readUnsignedByte();

			fileBinry.seek(pageHeader_array_offset);
			long point = fileBinry.getFilePointer();
			int address = fileBinry.readUnsignedShort();
			for (int i = 0; i < noOfColumns; i++) {

				fileBinry.seek(address);

				fileBinry.readUnsignedShort();
				int currentRowID = fileBinry.readInt();

				if (rowId == currentRowID) {
					cellOffset = address;
					retVal[0] = i;
					retVal[1] = cellOffset;
					return retVal;

				} else {

					point = (point + 2);
					fileBinry.seek(point);
					address = fileBinry.readUnsignedShort();
				}

			}

		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}

		return retVal;
	}

	public boolean isEmptyTable() throws IOException {
		return fileBinry.length() == 0;
	}

	public void insertNewRecord(Map<String, ArrayList<String>> token) throws Exception {
		currentPage = 1;
		int rowId = -1;
		if (isColumnSchema || isTableSchema) {
			tableKey = "rowid";
		}
		rowId = Integer.parseInt(token.get(tableKey).get(1));
		if (rowId < 0)
			throw new Exception("Insertion fails");

		searchLeafPage(rowId, false);

		insertNewRecordInPage(token, rowId, currentPage);

		routeOfLeafPage.clear();

	}

	private void insertNewRecordInPage(Map<String, ArrayList<String>> token, int rowId, int pageNumber) {
		readPageHeader(pageNumber);

		long no_of_Bytes = payloadSizeInBytes(token);
		long cellStartOffset = 0;
		try {
			fileBinry.seek(START_OF_CELL_HEADER);

			cellStartOffset = ((long) fileBinry.readUnsignedShort()) - (no_of_Bytes + 6);

		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}
		if (cellStartOffset < pageHeader_offset + 2) {

			LinkedList<byte[]> page1Cells = new LinkedList<>();
			LinkedList<byte[]> page2Cells = new LinkedList<>();
			try {
				fileBinry.seek(NO_OF_CELLS_HEADER);
				int no_of_Cells = fileBinry.readUnsignedByte();

				int splitCells = no_of_Cells / 2;
				int loc = 0;
				// long point = pageHeader_array_offset;
				splitCells = 1;

				long point = pageHeader_offset - 2;

				fileBinry.seek(point);

				fileBinry.seek(fileBinry.readUnsignedShort());

				fileBinry.readUnsignedShort();

				int currenRowID = fileBinry.readInt();
				while ((currenRowID > rowId)) {
					splitCells++;
					point = point - 2;
					fileBinry.seek(point);
					fileBinry.seek(fileBinry.readUnsignedShort());
					fileBinry.readUnsignedShort();
					currenRowID = fileBinry.readInt();
				}

				if (point == pageHeader_offset - 2) {
					splitCells = 0;
					// No need of split the current page
					if (currentPage == 1) {
						point = pageHeader_array_offset;
						for (int i = 1; i <= no_of_Cells; i++) {

							fileBinry.seek(point);
							loc = fileBinry.readUnsignedShort();

							fileBinry.seek(point);
							fileBinry.writeShort(0);

							point = fileBinry.getFilePointer();

							fileBinry.seek(loc);
							fileBinry.readUnsignedShort();

							fileBinry.seek(loc);
							byte[] cell = readCell(loc);
							page1Cells.add(cell);

						}

					}

				} else {
					// split the page

					if (currentPage == 1) {
						point = pageHeader_array_offset;
						for (int i = 1; i <= no_of_Cells - splitCells; i++) {

							fileBinry.seek(point);
							loc = fileBinry.readUnsignedShort();

							fileBinry.seek(point);
							fileBinry.writeShort(0);

							point = fileBinry.getFilePointer();

							fileBinry.seek(loc);
							fileBinry.readUnsignedShort();

							fileBinry.seek(loc);
							byte[] cell = readCell(loc);

							page1Cells.add(cell);
						}
					}

					for (int i = splitCells; i <= 1; i--) {

						point = pageHeader_offset - (2 * i);
						fileBinry.seek(point);
						loc = fileBinry.readUnsignedShort();

						fileBinry.seek(point);
						fileBinry.writeShort(0);

						fileBinry.seek(loc);
						byte[] cell = readCell(loc);

						page2Cells.add(cell);
					}
				}

				int rowIdMiddle = 0;
				if (currenRowID > rowId) {
					rowIdMiddle = currenRowID;
				} else {
					rowIdMiddle = rowId;
				}

				if (splitCells > 0) {
					fileBinry.seek(NO_OF_CELLS_HEADER);
					int noOfcells = fileBinry.readUnsignedByte();
					fileBinry.seek(NO_OF_CELLS_HEADER);
					fileBinry.writeByte(noOfcells - splitCells);
				}

				// split the page;
				int[] pageNumbers = splitLeafPage(page1Cells, page2Cells);

				// write Right Sibling for both pages
				fileBinry.seek(((pageNumbers[0] * pageSize) - pageSize) + 4);
				int prevRight = fileBinry.readInt();
				fileBinry.seek(((pageNumbers[0] * pageSize) - pageSize) + 4);
				fileBinry.writeInt(pageNumbers[1]);
				fileBinry.seek(((pageNumbers[1] * pageSize) - pageSize) + 4);
				fileBinry.writeInt(prevRight);

				// ch16PageFileExample.displayBinaryHex(binaryFile);

				// Interior Page Logic

				if (routeOfLeafPage.size() > 0 && routeOfLeafPage.get(routeOfLeafPage.size() - 1) > 0) {

					// if interior page exist
					writeToInterior(routeOfLeafPage.remove(routeOfLeafPage.size() - 1), pageNumbers[0], rowIdMiddle,
							pageNumbers[1]);

				} else {
					// create new interior page
					currentPage = 1;
					createNewInterior(pageNumbers[0], rowIdMiddle, pageNumbers[1]);

				}

				if (rowId < rowIdMiddle) {
					currentPage = pageNumbers[0];
				} else {
					currentPage = pageNumbers[1];
				}
				insertNewRecordInPage(token, rowId, currentPage);

			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}
		} else {

			writeCell(currentPage, token, cellStartOffset, no_of_Bytes);
		}
	}

	private boolean searchLeafPage(int rowId, boolean isFound) {

		routeOfLeafPage.add(currentPage);
		readPageHeader(currentPage);
		if (isLeafPage) {

			routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
			return true;
		} else {
			try {
				fileBinry.seek(NO_OF_CELLS_HEADER);

				int noOfColumns = fileBinry.readUnsignedByte();

				fileBinry.seek(pageHeader_array_offset);
				long currentArrayElementOffset = fileBinry.getFilePointer();
				int address;
				for (int i = 0; i < noOfColumns; i++) {
					fileBinry.seek(currentArrayElementOffset);
					address = fileBinry.readUnsignedShort();
					currentArrayElementOffset = fileBinry.getFilePointer();
					fileBinry.seek(address);
					int pageNumber = fileBinry.readInt();
					int delimiterRowId = fileBinry.readInt();
					if (rowId < delimiterRowId) {
						currentPage = pageNumber;
						isFound = searchLeafPage(rowId, false);

						break;
					}
				}

				if (!isFound) {
					fileBinry.seek(pageHeader_Offset_rightPagePointer);
					currentPage = fileBinry.readInt();
					isFound = searchLeafPage(rowId, false);
				}

			} catch (IOException e) {
				System.out.println("Unexpected Error");
			}
			return isFound;
		}

	}

	private byte[] readCell(int loc) {

		try {
			fileBinry.seek(loc);

			int payloadLength = fileBinry.readUnsignedShort();

			byte[] b = new byte[6 + payloadLength];
			fileBinry.seek(loc);

			fileBinry.read(b);
			fileBinry.seek(loc);
			fileBinry.write(new byte[6 + payloadLength]);

			return b;
		} catch (Exception e) {
			System.out.println("Unexpected Error");
		}

		return null;

	}

	private LinkedHashMap<String, ArrayList<String>> populateData(long cellOffset,
			LinkedHashMap<String, ArrayList<String>> token) {

		ArrayList<String> arrayOfValues = new ArrayList<String>();
		try {
			fileBinry.seek(cellOffset);
			int payLoadSize = fileBinry.readUnsignedShort();
			Integer actualRowID = fileBinry.readInt();
			short noOfColumns = fileBinry.readByte();
			payLoadSize -= 1;
			long offsetForSerialType = fileBinry.getFilePointer();
			long offSetForData = (offsetForSerialType + noOfColumns);
			int i = 0;
			for (String key : token.keySet()) {

				if (i == 0) {
					arrayOfValues.add(actualRowID.toString());
					token.put(key, new ArrayList<String>(arrayOfValues));
					i++;
					arrayOfValues.clear();
					continue;
				}

				fileBinry.seek(offsetForSerialType);
				short b = fileBinry.readByte();
				offsetForSerialType = fileBinry.getFilePointer();

				if (b == 0) {

					fileBinry.seek(offSetForData);
					int p = (fileBinry.readUnsignedByte());
					arrayOfValues.add("NULL");
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 1) {

					fileBinry.seek(offSetForData);
					int p = (fileBinry.readUnsignedShort());
					arrayOfValues.add("NULL");
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 2) {
					fileBinry.seek(offSetForData);
					int p = (fileBinry.readInt());
					arrayOfValues.add("NULL");
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 3) {

					fileBinry.seek(offSetForData);
					int p = (int) (fileBinry.readDouble());
					arrayOfValues.add("NULL");
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 12) {
					arrayOfValues.add("NULL");
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 4) {
					fileBinry.seek(offSetForData);
					arrayOfValues.add(Integer.toString(fileBinry.readUnsignedByte()));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 5) {
					fileBinry.seek(offSetForData);
					arrayOfValues.add(Integer.toString(fileBinry.readUnsignedShort()));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 6) {
					fileBinry.seek(offSetForData);
					arrayOfValues.add(Integer.toString(fileBinry.readInt()));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 7) {
					fileBinry.seek(offSetForData);
					arrayOfValues.add(Long.toString(fileBinry.readLong()));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 8) {

					fileBinry.seek(offSetForData);
					arrayOfValues.add(Float.toString(fileBinry.readFloat()));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 9) {

					fileBinry.seek(offSetForData);
					arrayOfValues.add(Double.toString(fileBinry.readDouble()));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 10) {
					fileBinry.seek(offSetForData);
					// arrayOfValues.add(Long.toString(binaryFile.readLong()));

					long timeInEpoch = fileBinry.readLong();
					Instant ii = Instant.ofEpochSecond(timeInEpoch);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
					Date date = Date.from(zdt2.toInstant());
					arrayOfValues.add(sdf.format(date));

					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 11) {
					fileBinry.seek(offSetForData);
					long timeInEpoch = fileBinry.readLong();
					Instant ii = Instant.ofEpochSecond(timeInEpoch);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

					Date date = Date.from(zdt2.toInstant());
					arrayOfValues.add(sdf.format(date));
					offSetForData = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else {
					byte[] text = new byte[b - 12];
					fileBinry.seek(offSetForData);

					fileBinry.read(text);
					arrayOfValues.add(new String(text));
					offSetForData = fileBinry.getFilePointer();

					token.put(key, new ArrayList<String>(arrayOfValues));

				}
				arrayOfValues.clear();
			}

		} catch (Exception e) {
			System.out.println("Unexpected Error");
		}

		return token;
	}

	private int[] splitLeafPage(LinkedList<byte[]> page1Cells, LinkedList<byte[]> page2Cells) {

		int[] pageNumbers = new int[2];

		// add existing Page
		try {
			if (currentPage != 1) {
				pageNumbers[0] = currentPage;
				pageHeader_offset = pageHeader_array_offset;
				if (page1Cells.size() > 0) {
					fileBinry.seek(START_OF_CELL_HEADER);
					fileBinry.writeShort(currentPage * (pageSize));
				}
				for (byte[] s : page1Cells) {

					long cellStartOffset = 0;

					fileBinry.seek(START_OF_CELL_HEADER);

					cellStartOffset = ((long) fileBinry.readUnsignedShort()) - (s.length);
					writeCellInBytes(currentPage, s, cellStartOffset);

				}
			} else {

				// create new Page
				lastPage += 1;

				pageNumbers[0] = lastPage;
				currentPage = lastPage;
				createPage(page1Cells);
			}
		} catch (IOException e) {
			System.out.println("Unexpected Error");

		}

		// create new Page
		lastPage += 1;

		pageNumbers[1] = lastPage;
		currentPage = lastPage;
		createPage(page2Cells);
		return pageNumbers;

	}

	// token<ColumnName, <data_type,value>>
	private void writeCell(int pageLocation, Map<String, ArrayList<String>> token, long cellStartOffset,
			long no_of_Bytes) {

		try {
			fileBinry.seek(START_OF_CELL_HEADER);
			fileBinry.writeShort((int) cellStartOffset);

			fileBinry.seek(NO_OF_CELLS_HEADER);
			short current_Cell_size = fileBinry.readByte();
			fileBinry.seek(NO_OF_CELLS_HEADER);
			fileBinry.write(current_Cell_size + 1);

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		writeToHeaderArray(cellStartOffset, Integer.parseInt(token.get(tableKey).get(1)));
		try {
			fileBinry.seek(cellStartOffset);
			/**
			 * Write cell header
			 */
			int rowId_or_pageNo = Integer.parseInt(token.get(tableKey).get(1));
			fileBinry.writeShort((int) no_of_Bytes);
			fileBinry.writeInt(rowId_or_pageNo);
		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}

		writeCellContent(pageLocation, token);
	}

	private void writeCellInBytes(int pageLocation, byte[] b, long cellStartOffset) {

		try {
			fileBinry.seek(START_OF_CELL_HEADER);
			fileBinry.writeShort((int) cellStartOffset);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		byte[] rowId = Arrays.copyOfRange(b, 2, 6);
		int id = java.nio.ByteBuffer.wrap(rowId).getInt();
		writeToHeaderArray(cellStartOffset, id);
		try {
			fileBinry.seek(cellStartOffset);
			fileBinry.write(b);
		} catch (IOException e) {
			System.out.println("Unexpected Error");
		}

	}

	private long payloadSizeInBytes(Map<String, ArrayList<String>> token) {
		long no_of_Bytes = 0;
		for (String key : token.keySet()) {
			if (key.equals(tableKey))
				continue; // Primary Key not needed in payload
			ArrayList<String> data_type = token.get(key);

			switch (data_type.get(0).trim().toLowerCase()) {
			case TINYINT:
				no_of_Bytes += 1;
				break;
			case SMALLINT:
				no_of_Bytes += 2;
				break;
			case INT:
				no_of_Bytes += 4;
				break;
			case BIGINT:
				no_of_Bytes += 8;
				break;
			case REAL:
				no_of_Bytes += 4;
				break;
			case DOUBLE:
				no_of_Bytes += 8;
				break;
			case DATETIME:
				no_of_Bytes += 8;
				break;
			case DATE2:
				no_of_Bytes += 8;
				break;
			case TEXT:
				no_of_Bytes += data_type.get(1).length() + 10;
				break;
			}

		}
		// 14
		no_of_Bytes += token.size(); // 1-byte TINYINT for no of columns + n
		// byte serial-type-code
		return no_of_Bytes;
	}

	private void writeToHeaderArray(long cellStartOffset, int rowID) {

		try {
			fileBinry.seek(NO_OF_CELLS_HEADER);
			int No_OfCells = fileBinry.readUnsignedByte();
			// pageHeader_offset = binaryFile.getFilePointer();

			int pos = 0;
			for (int i = 0; i < No_OfCells; i++) {
				fileBinry.seek((currentPage * pageSize - pageSize + 8) + (2 * i));
				fileBinry.seek(fileBinry.readUnsignedShort() + 2);
				if (rowID < fileBinry.readInt()) {
					pos = i;
					break;
				}
			}
			while (pos < No_OfCells) {
				fileBinry.seek((currentPage * pageSize - pageSize + 8) + (2 * (No_OfCells - 1)));
				fileBinry.writeShort(fileBinry.readUnsignedShort());
				No_OfCells--;
			}
			fileBinry.seek((currentPage * pageSize - pageSize + 8) + (2 * (pos)));
			fileBinry.writeShort((int) cellStartOffset);

		}

		catch (Exception e) {
			System.out.println("Unexpected Error");
		}
	}

	private void writeCellContent(int pageLocation2, Map<String, ArrayList<String>> token) {
		try {

			fileBinry.write(token.size() - 1);// no of columns

			writeSrl(token);

			writePayload(token);
		} catch (Exception e) {
			System.out.println("Unexpected Error");
		}

	}

	private void writePayload(Map<String, ArrayList<String>> token) throws IOException, UnsupportedEncodingException {
		// Payload

		for (String key : token.keySet()) {
			if (key.equals(tableKey))
				continue; // primary key not needed

			ArrayList<String> data_type = token.get(key);

			switch (data_type.get(0).trim().toLowerCase()) {

			case TINYINT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(Integer.parseInt(data_type.get(1)));
				} else {
					fileBinry.write(128);

				}

				break;
			case SMALLINT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.writeShort(Integer.parseInt(data_type.get(1)));
				} else {
					fileBinry.writeShort(-1);
				}

				break;
			case INT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.writeInt(Integer.parseInt(data_type.get(1)));
				} else {
					fileBinry.writeInt(-1);
				}
				break;
			case BIGINT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.writeLong(Long.parseLong(data_type.get(1)));
				} else {
					fileBinry.writeLong(-1);
				}

				break;
			case REAL:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.writeFloat(Float.parseFloat((data_type.get(1))));
				} else {
					fileBinry.writeFloat(-1);
				}

				break;
			case DOUBLE:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.writeDouble(Double.parseDouble((data_type.get(1))));
				} else {
					fileBinry.writeDouble(-1);
				}

				break;
			case DATETIME:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {

					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

					Date date;
					try {
						date = df.parse(data_type.get(1));

						ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

						long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;

						fileBinry.writeLong(epochSeconds);
					} catch (ParseException e) {
						System.out.println("Unexpected Error");
					}
				} else {
					fileBinry.writeLong(-1);

				}

				break;
			case DATE2:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {

					SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");

					Date date;
					try {
						date = d.parse(data_type.get(1));

						ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

						long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;

						fileBinry.writeLong(epochSeconds);
					} catch (ParseException e) {
						System.out.println("Unexpected Error");
					}
				} else {
					fileBinry.writeLong(-1);

				}

				break;
			case TEXT:
				if (data_type.get(1) != null) {
					String s = data_type.get(1);
					byte[] b = s.getBytes("UTF-8");
					for (byte bb : b)
						fileBinry.write(bb);
				}

				break;

			}

		}
	}

	private LinkedHashMap<String, ArrayList<String>> populateResult(ArrayList<String> searchCond, long cellOffset,
			LinkedHashMap<String, ArrayList<String>> token) {

		ArrayList<String> arrayOfValues = new ArrayList<String>();
		try {
			fileBinry.seek(cellOffset);
			int payLoadSize = fileBinry.readUnsignedShort();
			Integer actualRowID = fileBinry.readInt();
			short noOfColumns = fileBinry.readByte();
			payLoadSize -= 1;
			long offsetForSerialType = fileBinry.getFilePointer();
			long offset = (offsetForSerialType + noOfColumns);

			boolean matchFound = false;
			int i = 0;

			String seachCol = searchCond.get(0);
			String searchDataType = searchCond.get(1);
			String valueToBeSearched = searchCond.get(2);

			String value = null;

			long offsetForSerialTypeMatch = offsetForSerialType;
			long offSetForDataMatch = (offset);

			int colIndex = Integer.parseInt(seachCol);

			int currentColIndex = 1;

			for (String key : token.keySet()) {

				fileBinry.seek(offsetForSerialType);
				short b = fileBinry.readByte();
				offsetForSerialType = fileBinry.getFilePointer();
				if (b == 0) {

					fileBinry.seek(offset);
					int p = (fileBinry.readUnsignedByte());
					value = "NULL";
					offset = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 1) {

					fileBinry.seek(offset);
					int p = (fileBinry.readUnsignedShort());
					value = "NULL";
					offset = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 2) {
					fileBinry.seek(offset);
					int p = (fileBinry.readInt());
					value = "NULL";
					offset = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));
				} else if (b == 3) {

					fileBinry.seek(offset);
					int p = (int) (fileBinry.readDouble());
					value = "NULL";
					offset = fileBinry.getFilePointer();
					token.put(key, new ArrayList<String>(arrayOfValues));

				} else if (b == 12) {
					value = "NULL";

				} else if (b == 4) {
					fileBinry.seek(offset);
					value = Integer.toString(fileBinry.readUnsignedByte());
					offset = fileBinry.getFilePointer();
				} else if (b == 5) {
					fileBinry.seek(offset);
					value = (Integer.toString(fileBinry.readUnsignedShort()));
					offset = fileBinry.getFilePointer();
				} else if (b == 6) {
					fileBinry.seek(offset);
					value = (Integer.toString(fileBinry.readInt()));
					offset = fileBinry.getFilePointer();
				} else if (b == 7) {
					fileBinry.seek(offset);
					value = (Long.toString(fileBinry.readLong()));
					offset = fileBinry.getFilePointer();
				} else if (b == 8) {

					fileBinry.seek(offset);
					value = (Float.toString(fileBinry.readFloat()));
					offset = fileBinry.getFilePointer();
				} else if (b == 9) {

					fileBinry.seek(offset);
					value = (Double.toString(fileBinry.readDouble()));
					offset = fileBinry.getFilePointer();

				} else if (b == 10) {
					fileBinry.seek(offset);

					long timeInEpoch = fileBinry.readLong();

					value = Long.toString(timeInEpoch);
					offset = fileBinry.getFilePointer();
				} else if (b == 11) {
					fileBinry.seek(offset);

					long timeInEpoch = fileBinry.readLong();
					value = Long.toString(timeInEpoch);

					// value = (Long.toString(binaryFile.readLong()));
					offset = fileBinry.getFilePointer();
				} else {
					byte[] text = new byte[b - 12];
					fileBinry.seek(offset);

					fileBinry.read(text);
					value = (new String(text));
					offset = fileBinry.getFilePointer();

				}

				if (currentColIndex == colIndex) {

					switch (searchDataType.trim().toLowerCase()) {

					case TINYINT:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& Integer.parseInt(valueToBeSearched) == Integer.parseInt(value)) {
							matchFound = true;
						}
						break;
					case SMALLINT:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& Integer.parseInt(valueToBeSearched) == Integer.parseInt(value)) {
							matchFound = true;
						}
						break;
					case INT:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& Integer.parseInt(valueToBeSearched) == Integer.parseInt(value)) {
							matchFound = true;
						}
						break;
					case BIGINT:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& Long.parseLong(valueToBeSearched) == Long.parseLong(value)) {
							matchFound = true;
						}
						break;
					case REAL:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& Float.parseFloat(valueToBeSearched) == Float.parseFloat(value)) {
							matchFound = true;
						}
						break;
					case DOUBLE:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& Double.parseDouble(valueToBeSearched) == Double.parseDouble(value)) {
							matchFound = true;
						}
						break;
					case DATETIME:
						long epochSeconds = 0;

						if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
							break;
						}

						if (value != null && !value.equalsIgnoreCase("null")) {
							SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

							Date date;
							try {
								date = df.parse(valueToBeSearched);

								ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

								epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
							} catch (Exception e) {

							}

						}

						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& (epochSeconds) == Long.parseLong(value)) {

							Instant ii = Instant.ofEpochSecond(epochSeconds);
							ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
							Date date = Date.from(zdt2.toInstant());
							value = sdf.format(date);

							matchFound = true;
						}
						break;
					case DATE2:
						long epocSecs = 0;
						if (value != null && value.equalsIgnoreCase("null")
								&& value.equalsIgnoreCase(valueToBeSearched)) {
							matchFound = true;
							break;
						}

						if (value != null && !value.equalsIgnoreCase("null")) {
							SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

							Date date;
							try {
								date = df.parse(valueToBeSearched);

								ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

								epocSecs = zdt.toInstant().toEpochMilli() / 1000;
							} catch (Exception e) {

							}

						}

						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null && !value.equalsIgnoreCase("null")
								&& (epocSecs) == Long.parseLong(value)) {

							Instant ii = Instant.ofEpochSecond(epocSecs);
							ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							Date date = Date.from(zdt2.toInstant());
							value = sdf.format(date);
							matchFound = true;
						}
						break;
					case TEXT:
						if (value == null && value == valueToBeSearched) {
							matchFound = true;
						} else if (value != null && valueToBeSearched != null
								&& valueToBeSearched.equalsIgnoreCase(value)) {
							matchFound = true;
						}

						break;
					}

					break;
				}
				currentColIndex++;
			}

			if (matchFound) {
				offsetForSerialType = offsetForSerialTypeMatch;
				offset = offSetForDataMatch;

				for (String key : token.keySet()) {

					if (i == 0) {
						arrayOfValues.add(actualRowID.toString());
						token.put(key, new ArrayList<String>(arrayOfValues));
						i++;
						arrayOfValues.clear();
						continue;
					}

					fileBinry.seek(offsetForSerialType);
					short b = fileBinry.readByte();
					offsetForSerialType = fileBinry.getFilePointer();
					if (b == 0) {

						fileBinry.seek(offset);
						int p = (fileBinry.readUnsignedByte());
						arrayOfValues.add("NULL");
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

					} else if (b == 1) {

						fileBinry.seek(offset);
						int p = (fileBinry.readUnsignedShort());
						arrayOfValues.add("NULL");
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

					} else if (b == 2) {
						fileBinry.seek(offset);
						int p = (fileBinry.readInt());
						arrayOfValues.add("NULL");
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 3) {

						fileBinry.seek(offset);
						int p = (int) (fileBinry.readDouble());
						arrayOfValues.add("NULL");
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

					} else if (b == 12) {
						arrayOfValues.add("NULL");
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 4) {
						fileBinry.seek(offset);
						arrayOfValues.add(Integer.toString(fileBinry.readUnsignedByte()));
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 5) {
						fileBinry.seek(offset);
						arrayOfValues.add(Integer.toString(fileBinry.readUnsignedShort()));
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 6) {
						fileBinry.seek(offset);
						arrayOfValues.add(Integer.toString(fileBinry.readInt()));
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 7) {
						fileBinry.seek(offset);
						arrayOfValues.add(Long.toString(fileBinry.readLong()));
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 8) {

						fileBinry.seek(offset);
						arrayOfValues.add(Float.toString(fileBinry.readFloat()));
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 9) {

						fileBinry.seek(offset);
						arrayOfValues.add(Double.toString(fileBinry.readDouble()));
						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

					} else if (b == 10) {
						fileBinry.seek(offset);

						Instant ii = Instant.ofEpochSecond(fileBinry.readLong());
						ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
						Date date = Date.from(zdt2.toInstant());
						arrayOfValues.add(sdf.format(date));

						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else if (b == 11) {
						fileBinry.seek(offset);
						// arrayOfValues.add(Long.toString(binaryFile.readLong()));

						Instant ii = Instant.ofEpochSecond(fileBinry.readLong());
						ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						Date date = Date.from(zdt2.toInstant());

						arrayOfValues.add(sdf.format(date));

						offset = fileBinry.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
					} else {
						byte[] text = new byte[b - 12];
						fileBinry.seek(offset);

						fileBinry.read(text);
						arrayOfValues.add(new String(text));
						offset = fileBinry.getFilePointer();

						token.put(key, new ArrayList<String>(arrayOfValues));

					}
					arrayOfValues.clear();
				}
			}

			if (!matchFound)
				token = null;

		} catch (Exception e) {
			System.out.println("Unexpected Error");
		}

		return token;
	}

	/**
	 * FYI: Header format - https://sqlite.org/fileformat2.html
	 */
	private void writePageHeader(int pageLocation, boolean isLeaf, int no_of_Cells, int rightPage) {
		int type = LEAF;
		int pointer = -1;
		if (!isLeaf) {
			type = INTERNAL;
			pointer = rightPage;
			no_of_Cells = 0;
		}
		try {
			fileBinry.seek((pageLocation - 1) * pageSize);

			fileBinry.write(type);
			NO_OF_CELLS_HEADER = fileBinry.getFilePointer();
			fileBinry.write(no_of_Cells);
			START_OF_CELL_HEADER = fileBinry.getFilePointer();
			fileBinry.writeShort((int) (pageLocation * pageSize));
			pageHeader_Offset_rightPagePointer = fileBinry.getFilePointer();
			fileBinry.writeInt(pointer);
			pageHeader_array_offset = fileBinry.getFilePointer();
			pageHeader_offset = pageHeader_array_offset;
		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}
	}

	private void readPageHeader(int pageLocation) {
		try {
			int currentPageIdx = currentPage - 1;
			fileBinry.seek(currentPageIdx * pageSize);

			int flag = fileBinry.readUnsignedByte();

			isLeafPage = flag == LEAF;

			NO_OF_CELLS_HEADER = (currentPageIdx * pageSize) + NODE_TYPE_OFFSET;
			int noOfCells = fileBinry.readUnsignedByte();
			START_OF_CELL_HEADER = fileBinry.getFilePointer();
			fileBinry.readUnsignedShort();
			pageHeader_Offset_rightPagePointer = fileBinry.getFilePointer();
			fileBinry.readInt();
			pageHeader_array_offset = fileBinry.getFilePointer();
			pageHeader_offset = fileBinry.getFilePointer() + (2 * noOfCells);

		} catch (Exception e) {
			System.out.println("Unexpected Error" + e.getMessage());

		}

	}

	public boolean close() {
		try {
			fileBinry.close();
			return true;
		} catch (IOException e) {
			System.out.println("Unexpected Error");
			return false;
		}
	}

	private void createPage(LinkedList<byte[]> pageCells) {
		try {
			fileBinry.setLength(pageSize * currentPage);
			writePageHeader(currentPage, true, pageCells.size(), -1);
			readPageHeader(currentPage);

			pageHeader_offset = pageHeader_array_offset;
			ListIterator<byte[]> iterator = pageCells.listIterator(pageCells.size());

			long cellStartOffset = 0;

			fileBinry.seek(START_OF_CELL_HEADER);
			fileBinry.writeShort(currentPage * (pageSize));
			while (iterator.hasPrevious()) {
				byte[] s = iterator.previous();

				fileBinry.seek(START_OF_CELL_HEADER);

				cellStartOffset = ((long) fileBinry.readUnsignedShort()) - (s.length);
				writeCellInBytes(currentPage, s, cellStartOffset);

			}
		} catch (Exception e) {
			System.out.println("Unexpected Error");
		}
	}

	private void writeSrl(Map<String, ArrayList<String>> token) throws IOException {
		// n - bytes Serial code Types , one for each column
		for (String key : token.keySet()) {
			if (key.equals(tableKey))
				continue; // primary key not needed
			ArrayList<String> data_type = token.get(key);

			switch (data_type.get(0).trim().toLowerCase()) {

			case TINYINT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(4);
				} else {
					fileBinry.write(0);
				}
				break;
			case SMALLINT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(5);
				} else {
					fileBinry.write(1);
				}
				break;
			case INT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(6);
				} else {
					fileBinry.write(2);
				}
				break;
			case BIGINT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(7);
				} else {
					fileBinry.write(3);
				}
				break;
			case REAL:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(8);
				} else {
					fileBinry.write(2);
				}
				break;
			case DOUBLE:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(9);
				} else {
					fileBinry.write(3);
				}
				break;
			case DATETIME:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(10);
				} else {
					fileBinry.write(3);
				}
				break;
			case DATE2:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(11);
				} else {
					fileBinry.write(3);
				}
				break;
			case TEXT:
				if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
					fileBinry.write(12 + (data_type.get(1).length()));
				} else {
					fileBinry.write(12);
				}
				break;

			}

		}
	}

	public String getPrimaryKey() {
		return tableKey;
	}

}
