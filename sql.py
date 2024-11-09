"SQLite Widget for Textual"

from __future__ import annotations
from typing import Sequence, Any

from contextlib import contextmanager
import shutil
from pathlib import Path
from platformdirs import user_data_dir
from importlib.resources import files
from importlib import import_module
import sqlite3

from textual.widget import Widget


class SQLite(Widget):
    """A simple SQLite database wrapper for TextualDon.   
    Cannot attach child widgets (blocked)."""


    def __init__(self, pkg_name: str, db_filename: str = None, **kwargs):
        """Create a new SQLite database wrapper.   
        Must pass in the name of the package that contains the database scaffold.   
        Optionally pass in the name of the database file. If not provided, the name will be `<pkg_name>.db`.
        
        EXAMPLE:
        ```
        db = SQLite("my_package")
        ```
        This would assume that my_package has a file called `my_package.db` in its resources directory.

        Args:
            pkg_name: The name of the package that contains the database file.
            db_filename: The name of the database file. If None, name is `<pkg_name>.db`.
            name: The name of the widget.
            id: The ID of the widget in the DOM.
            classes: The CSS classes for the widget.
            disabled: Whether the widget is disabled or not.

        Raises:
            ModuleNotFoundError: If the package name is not found.
            FileNotFoundError: If the package does not contain the expected resources.
        """

        try:
            import_module(pkg_name)     # validate the package name
            files(pkg_name)             # validate the package has resources
        except ModuleNotFoundError:
            raise ModuleNotFoundError(f"Package {pkg_name} not found. Please install it first.")
        except FileNotFoundError:
            raise FileNotFoundError(f"Package {pkg_name} does not contain the expected resources.")

        self.pkg_name = pkg_name
        self.db_filename = db_filename if db_filename else f"{pkg_name}.db"
        self.user_db_path = self.get_user_db()
        self.connection = sqlite3.connect(self.user_db_path)

        super().__init__(**kwargs)


    def get_user_db(self) -> Path:
        """Used internally by class
        First time its run it will copy the database file to the user's data directory"""

        # platformdirs gives us platform specific directories
        data_dir = Path(user_data_dir(appname=self.pkg_name, ensure_exists=True))
        user_db_path = data_dir / self.db_filename 

        if not user_db_path.exists():
            original_db = files(self.pkg_name).joinpath(self.db_filename)
            shutil.copy2(original_db, user_db_path)

        return user_db_path

    @contextmanager
    def _cursor(self):
        """Used internally by class"""
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            

    def create_table(self, table: str, columns: dict[str, str]):
        """Create a new table in the database. Here for convenience but it's generally easier
        to externally scaffold the database with a SQLite tool such as Harlequin.

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




