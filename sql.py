"SQLite Widget"


# Standard library imports
from __future__ import annotations
from typing import TYPE_CHECKING, Sequence, Any
if TYPE_CHECKING:
    # from textual.app import ComposeResult
    # from textual.worker import Worker
    pass

from contextlib import contextmanager
# import os
import shutil
# import importlib
from pathlib import Path
from platformdirs import user_data_dir # user_config_dir
import pkg_resources
import sqlite3
# from configparser import ConfigParser



# Textual imports
# from textual import on, work
# from textual.containers import Container, Horizontal
from textual.widget import Widget
# from textual.widgets import Static
# from textual.reactive import reactive
# from textual.widgets import (
#     Input,
# )

# TextualDon imports
# from .simplebutton import SimpleButton
# from textualdon.messages import UpdateBannerMessage


class SQLite(Widget):
    """A simple SQLite database wrapper for TextualDon.   
    Cannot attach child widgets (blocked)."""


    def __init__(self, *, db_path: str, **kwargs):
        """Create a new SQLite database wrapper.

        Interface:
        ``` 
        insert_one(
            table: str,
            columns: list[str],
            values: list[Any],
            auto_commit: bool = True
            )

        delete_one(
            table_name: str,
            column_name: str,
            value: Any
            )

        update_column(
            table_name: str,
            column_name: str,
            new_value: Any,
            condition_column: str,
            condition_value: Any
            )

        fetchall(query: str, params: tuple = None)

        fetchone(query: str, params: tuple = None)

        close()
        ```

        Args:
            db_path: The path to the SQLite database file.
            name: The name of the widget.
            id: The ID of the widget in the DOM.
            classes: The CSS classes for the widget.
            disabled: Whether the widget is disabled or not.
        """

        self.user_db_path = self.get_user_db()
        self.connection = sqlite3.connect(self.user_db_path)

        super().__init__(**kwargs)


    def get_user_db(self) -> Path:
        """Used internally by class
        First time its run it will copy the database file to the user's data directory"""

        # platformdirs gives us platform specific directories
        data_dir = Path(user_data_dir(appname="textualdon", ensure_exists=True))
        user_db_path = data_dir / "textualdon.db" 

        if not user_db_path.exists():
            original_db = pkg_resources.resource_filename('textualdon', 'textualdon.db')
            shutil.copy2(original_db, user_db_path)
            print(f"Initialized database at: {user_db_path}")

        return user_db_path

    
    @contextmanager
    def _cursor(self):
        """Used internally by class"""
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def insert_one(self, table: str, columns: list[str], values: Sequence[Any], auto_commit: bool = True):
        """Insert a single row into the database.   
        Using the auto_commit parameter, you can stage multiple inserts before committing them all at once.
        Simply let it go back to the default value of True to commit all inserts.

        USAGE:
        ```    
        table = "users"
        columns = ["name", "age", "email"]
        values = ["Alice", 30, "alice@example.com"]
        db.insert_one(table, columns, values)
        ```
        Raw SQL:   
        INSERT INTO {table} ({', '.join(columns)}) VALUES (?, ?, ?);

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
            self.app.handle_exception(e)

    def delete_one(self, table_name: str, column_name: str, value: Any):
        """Delete a row from a database table.

        USAGE:
        ```
        delete_one('employees', 'id', 3)
        ```
        Raw SQL:
        DELETE FROM {table_name} WHERE {column_name} = ?; """
         
        sql_delete_query = f"DELETE FROM {table_name} WHERE {column_name} = ?;"

        try:
            with self._cursor() as cursor:
                cursor.execute(sql_delete_query, (value,))
            self.connection.commit()
            self.log.info(f"Successfully deleted {cursor.rowcount} row(s).")

        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            self.app.handle_exception(e)


    def update_column(
            self,
            table_name: str,
            column_name: str,
            new_value: Any,
            condition_column: str,
            condition_value: Any
        ):
        """Update a column in a database table.
        
        USAGE:
        ```
        update_column('employees', 'salary', 75000, 'id', 3)
        ``` 
        Raw SQL:   
        UPDATE {table_name} SET {column_name} = ? WHERE {condition_column} = ?; """
        
        sql_update_query = f"UPDATE {table_name} SET {column_name} = ? WHERE {condition_column} = ?;"

        try:
            with self._cursor() as cursor:
                cursor.execute(sql_update_query, (new_value, condition_value))
            self.connection.commit()
            self.log.info(f"Successfully updated {cursor.rowcount} row(s).")

        except sqlite3.DatabaseError as e:
            self.connection.rollback()
            self.app.handle_exception(e)


    def fetchall(self, query: str, params: Sequence = None) -> list[tuple]:
        """This method runs a SQL query and retrieves all rows that match the query criteria.
        
        USAGE:
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
            self.app.handle_exception(e)
    
    def fetchone(self, query: str, params: Sequence = None) -> tuple:
        """This method is similar to fetchall, but it only retrieves a single row
        from the database, even if multiple rows meet the query criteria.
        
        USAGE:
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




