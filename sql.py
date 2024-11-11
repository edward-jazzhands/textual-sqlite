"SQLite Widget for Textual"

# stdlib
from typing import Sequence, Any
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from importlib import resources

# 3rd party
from platformdirs import user_data_dir
from textual.widget import Widget


class SQLite(Widget):
    """A simple SQLite database wrapper for TextualDon.   
    Cannot attach child widgets (blocked).
    
    See __init__ for usage."""


    def __init__(
            self,
            app_name: str,
            sql_script: str,
            db_filename: str = None,
            **kwargs):
        """Create a new SQLite database wrapper.    
        Must pass in the name of the package that is using this widget (to locate your SQL script).   
        This same name will be used to create a directory in the user's data directory to store the database file.

        Args:
            app_name: The name of the package that is using this widget.
            sql_script: The name of the SQL script file to use for setting up the database tables and schema.   
                Path is relative to the main package directory.
            db_filename: The name the database file will use. If None, name will be `<app_name>.db`.
            name: The name of the widget.
            id: The ID of the widget in the DOM.
            classes: The CSS classes for the widget.
            disabled: Whether the widget is disabled or not.

        EXAMPLE:
        ```
        db = SQLite("my_package", "SQL/create_tables.sql")
        ```
        This would assume that my_package has a file called `create_tables.sql` in its `SQL` directory.   
        The widget will only run your SQL script the first time the database is created.
        """

        super().__init__(**kwargs)

        self.app_name = app_name
        self.sql_script = sql_script
        self.db_filename = db_filename or f"{self.app_name}.db"

        self.user_db_path = self.get_user_db()
        if not self.user_db_path.exists():
            self.connection = sqlite3.connect(self.user_db_path)
            self.initialize_db()
        else:
            self.connection = sqlite3.connect(self.user_db_path)

    def get_user_db(self) -> Path:

        # platformdirs gives us platform specific directories
        data_dir = Path(user_data_dir(appname=self.app_name, ensure_exists=True))
        user_db_path = data_dir / self.db_filename
        return user_db_path
    
    def initialize_db(self):

        sql_file_path = Path(resources.files(self.app_name) / self.sql_script)
        print(f"SQL file location: \n{sql_file_path} \n")
        with open(sql_file_path, 'r') as f:
            script = f.read()
            self.execute_script(script)

    @contextmanager
    def _cursor(self):
        """Used internally by class"""
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute_script(self, script: str):
        """Execute a SQL script on the database.

        EXAMPLE:
        ```
        script = "create_table.sql"
        db.execute_script(script)
        ``` """

        try:
            with self._cursor() as cursor:
                cursor.executescript(script)
            self.connection.commit()
        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            raise e

        print("Successfully executed script on database.")
            

    def create_table(self, table: str, columns: dict[str, str]):
        """Create a new table in the database. Here for convenience but it's generally recommended
        to use the SQL script to create tables.

        EXAMPLE:
        ```
        table = "users"
        columns = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "name": "TEXT",
            "age": "INTEGER",
            "email": "TEXT"
        }
        db.create_table(table, columns)
        ```
        Raw SQL:   
        `CREATE TABLE IF NOT EXISTS {table} ({', '.join( [f"{k} {v}" for k, v in columns.items()] ) }); `
        
        Args:
            table (str): The name of the table to create.
            columns (dict[str, str]): A dictionary of column names and types. """

        columns_str = ', '.join([f"{k} {v}" for k, v in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table} ({columns_str});"

        try:
            with self._cursor() as cursor:
                cursor.execute(query)
            self.connection.commit()
        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            raise e

        self.log.debug(f"Successfully created table {table} with columns {columns}.")

    def insert_one(self, table: str, columns: list[str], values: Sequence[Any], auto_commit: bool = True):
        """Insert a single row into the database.   
        Using the auto_commit parameter, you can stage multiple inserts before committing them all at once.
        Simply let it go back to the default value of True to commit all inserts.

        EXAMPLE:
        ```    
        table = "users"
        columns = ["name", "age", "email"]
        values = ["Alice", 30, "alice@example.com"]
        db.insert_one(table, columns, values)
        ```
        Raw SQL:   
        `INSERT INTO {table} ({', '.join(columns)}) VALUES (?, ?, ?);`

        Args:
            table (str): The name of the table to insert the row into.
            columns (list[str]): The list of column names.
            values (Sequence[Any]): List or tuple of values to insert.
            auto_commit (bool, optional): Whether to commit the transaction automatically. Defaults to True.
        """

        placeholders = ', '.join(['?'] * len(values))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            with self._cursor() as cursor:
                cursor.execute(query, values)   
                if auto_commit:
                    self.connection.commit()
        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            raise e

        self.log.debug(f"Successfully inserted a row into {table} with values {values}.")


    def delete_one(self, table_name: str, column_name: str, value: Any):
        """Delete a row from a database table.

        EXAMPLE:
        ```
        delete_one('employees', 'id', 3)
        ```
        Raw SQL:   
        `DELETE FROM {table_name} WHERE {column_name} = ?;` 
        
        Args:
            table_name (str): The name of the table to delete from.
            column_name (str): The column to use for the delete condition.
            value (Any): The value to use for the delete condition.
        """
         
        sql_delete_query = f"DELETE FROM {table_name} WHERE {column_name} = ?;"

        try:
            with self._cursor() as cursor:
                cursor.execute(sql_delete_query, (value,))
            self.connection.commit()
        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            raise e

        self.log.debug(f"Successfully deleted {table_name} row where {column_name} is {value}.")

    def update_column(
            self,
            table_name: str,
            column_name: str,
            new_value: Any,
            condition_column: str,
            condition_value: Any
        ):
        """Update a column in a database table.
        
        EXAMPLE:
        ```
        update_column('employees', 'salary', 75000, 'id', 3)
        ``` 
        Raw SQL:   
        `UPDATE {table_name} SET {column_name} = ? WHERE {condition_column} = ?; `
        
        Args:
            table_name (str): The name of the table to update.
            column_name (str): The name of the column to update.
            new_value (Any): The new value to set in the column.
            condition_column (str): The column to use for the update condition.
            condition_value (Any): The value to use for the update condition.
        """
        
        sql_update_query = f"UPDATE {table_name} SET {column_name} = ? WHERE {condition_column} = ?;"

        try:
            with self._cursor() as cursor:
                cursor.execute(sql_update_query, (new_value, condition_value))
            self.connection.commit()

        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            raise e

        self.log.debug(f"Successfully updated {table_name} column {column_name} to "
                       f"{new_value} where {condition_column} is {condition_value}.")


    def fetchall(self, query: str, params: Sequence = None) -> list[tuple]:
        """This method runs a SQL query and retrieves all rows that match the query criteria.
        
        EXAMPLE:
        ```
        query = "SELECT * FROM users WHERE name = ?"
        params = ("Alice")
        rows = db.fetchall(query, params)
        ``` 
        Args:
            query (str): The SQL query to run.
            params (Sequence, optional): The query parameters. Defaults to None.

        Returns:
            list[tuple]: A list of rows that match the query criteria.
        """

        with self._cursor() as cursor:
            cursor.execute(query, params or [])
            return cursor.fetchall()
        
        try:
            pass
        except Exception as e:
            raise e
    
    def fetchone(self, query: str, params: Sequence = None) -> tuple:
        """This method is similar to fetchall, but it only retrieves a single row
        from the database, even if multiple rows meet the query criteria.
        
        EXAMPLE:
        ```
        query = "SELECT * FROM users WHERE name = ?"
        params = ("Alice")
        row = db.fetchone(query, params)
        ``` 
        Args:
            query (str): The SQL query to run.
            params (Sequence, optional): The query parameters. Defaults to None.

        Returns:
            tuple: A single row that matches the query criteria.
        """

        with self._cursor() as cursor:
            cursor.execute(query, params or [])
            return cursor.fetchone()
    
    
    def close(self):
        """Close the database connection."""
        self.connection.close()




