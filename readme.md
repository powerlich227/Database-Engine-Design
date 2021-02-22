#Instructions for running the app:

- The Project was developed using Eclipse IDE and Java.

- Steps to Run:
	- Extract the compressed zip folder downloaded from eLearning. 
	- Import the folder (Import existing projects) into the Eclipse IDE.
	- Run the MyDatabase.java to start the application.

- Following commands are supported:


	| CREATE TABLE table_name (row_id INT PRIMARY KEY, column_name1 data_type2 [NOT NULL][UNIQUE][AUTOINCREMENT][DEFAULT],... ); | Create a new table schema if not already exist |
	SELECT * FROM table_name;                        																		   | Display all records in the table
	SELECT * FROM table_name WHERE <column_name> = <value>;                                                                    | Display records whose column is <value>
	UPDATE <table_name> SET column_name = value WHERE <row_id = value>;                                                        | Modifies one or more records in a table
	INSERT INTO <table_name> (column_list) VALUES (value1, value2, value3,..);                                                 | Insert a new record into the indicated table
	DELETE FROM <table_name> WHERE row_id = key_value;                                                                         | Delete a single row/record from a table given the row_id primary key
	DROP TABLE table_name;                                                                                                     | Remove table data and its schema
	SHOW TABLES;                                     	                                                                       | Displays a list of all tables in the Database
	VERSION;                                                                                                                   | Show the program version
	HISTORY;                                                                                                                   | Show all recent commands
	HELP;                                            																		   | Show this help information
	EXIT;                                            																		   | Exit the program
