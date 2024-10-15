import pandas as pd
import uuid
from datetime import datetime, date
from typing import Type, Optional, List
from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy.orm import session as sess
from sqlalchemy.sql import literal
from sqlalchemy.inspection import inspect

from stock.src.models import Base, Ticker


@dataclass
class TickerServiceBase:
    """
    :param session: SQLAlchemy session for database operations.
    :param symbol: The symbol of the ticker to handle.
    :param ticker: The Ticker object for the symbol.
    """
    session: sess.Session
    symbol: str
    ticker: Ticker = field(default=None, init=False)

    def initialize_ticker(self, ticker: Ticker) -> None:
        """
        Initialize the Ticker object for the symbol.

        :param ticker: The Ticker object for the symbol.
        """
        self.ticker = ticker

    """Generic record update handler for database operations."""
    def handle_generic_record_update(
            self,
            new_record_data: dict,
            model_class: Type[Base],
            additional_filters: Optional[list] = None,
            comparison_fields: Optional[list[str]] = None,
            print_no_changes: bool = True
    ) -> bool:
        """
        Handle the insertion or update of a generic record in the database.

        :param new_record_data: Dictionary containing the new record data.
        :param model_class: The SQLAlchemy model class to interact with.
        :param additional_filters: Optional list of additional filters for the query.
        :param comparison_fields: Optional list of fields to compare for detecting changes.
        :param print_no_changes: Whether to print a message when no changes are detected.
        """
        try:
            # Get the current timestamp for logging and record keeping
            current_timestamp = datetime.now()
            # Format the model class name for consistent and readable logging
            model_class_name = self.format_model_class_name(model_class)

            # Prepare the filter criteria for the query
            filters = self.prepare_filters(model_class, additional_filters)

            # Retrieve the most recent record that matches the filters
            last_record = self.fetch_last_record(model_class, filters)

            # Compare the new data with the last record and log any changes
            changes_log = self.compare_and_log_changes(last_record, new_record_data, model_class_name)

            has_changes = bool(changes_log)

            if has_changes or not last_record:
                # Create and add a new record if there are changes or no previous record exists
                self.create_new_record(new_record_data, model_class, current_timestamp)

                # Commit changes to the session and print the changes log
                self.commit_changes(last_record, model_class_name, changes_log)

                return True

            # Log that no changes were detected if specified
            # self.log_no_changes(model_class_name, print_no_changes)
            return False

        except Exception as e:
            # Rollback the transaction in case of an error
            self.session.rollback()
            print(f"Error occurred: {e}")
            return False

    @staticmethod
    def format_model_class_name(model_class: Type[Base]) -> str:
        """
        Format the model class name for logging.

        :param model_class: The SQLAlchemy model class.
        :return: Formatted model class name.
        """
        return ' '.join([x.capitalize() for x in model_class.__tablename__.replace('_', ' ').split(' ')]).rjust(50)

    def prepare_filters(self, model_class: Type[Base], additional_filters: Optional[List]) -> List:
        """
        Prepare the filter criteria for the query.

        :param model_class: The SQLAlchemy model class.
        :param additional_filters: Optional list of additional filters for the query.
        :return: List of filter criteria.
        """
        filters = [model_class.ticker_id == literal(self.ticker.id)]
        if additional_filters:
            filters.extend(additional_filters)
        return filters

    def fetch_last_record(self, model_class: Type[Base], filters: List) -> Optional[Base]:
        """
        Fetch the last record for the ticker with the applied filters.

        :param model_class: The SQLAlchemy model class.
        :param filters: List of filter criteria.
        :return: The last record if found, otherwise None.
        """
        return self.session.query(model_class).filter(*filters).order_by(model_class.last_update.desc()).first()

    def compare_and_log_changes(self, last_record: Optional[Base], new_record_data: dict, model_class_name: str ) -> List[str]:
        """
        Compare the new data with the last record and log changes.

        :param last_record: The last record from the database.
        :param new_record_data: Dictionary containing the new record data.
        :param model_class_name: Formatted model class name for logging.
        :return: List of change logs.
        """
        changes_log = []
        # Compare the new data with the last record
        if last_record:
            for field_, new_value in new_record_data.items():
                old_value = getattr(last_record, field_)

                # Handling UUID comparisons
                if isinstance(old_value, uuid.UUID):
                    old_value = str(old_value)
                    new_value = str(new_value)

                # Handling datetime comparisons
                if isinstance(old_value, (datetime, date)) and isinstance(new_value, (datetime, date)):
                    old_value = old_value.date() if isinstance(old_value, datetime) else old_value
                    new_value = new_value.date() if isinstance(new_value, datetime) else new_value

                # Handling float comparisons to account for small differences
                if isinstance(new_value, float):
                    # Avoid detecting changes if both are NaN
                    if pd.isna(old_value) and pd.isna(new_value):
                        continue
                    old_value = float(old_value) if old_value is not None else old_value
                    new_value = float(new_value) if new_value is not None else new_value

                if new_value is not None and old_value != new_value:
                    ticker_print = f"{self.ticker.symbol} - " if len(changes_log) == 0 else ' ' * len(self.ticker.symbol) + "   "
                    model_class_print = model_class_name if len(changes_log) == 0 else ' ' * len(model_class_name)
                    log = f"{ticker_print}{model_class_print} - {field_} changed from {old_value} to {new_value} - new record data: {new_record_data}"
                    changes_log.append(log)

        return changes_log

    def create_new_record(self, new_record_data: dict, model_class: Type[Base], current_timestamp: datetime) -> None:
        """
        Create a new model instance with the new data.

        :param new_record_data: Dictionary containing the new record data.
        :param model_class: The SQLAlchemy model class to interact with.
        :param current_timestamp: Current timestamp for the last_update field.
        """
        new_record_data['ticker_id'] = self.ticker.id
        new_record_data['last_update'] = current_timestamp
        new_record = model_class(**new_record_data)
        self.session.add(new_record)

    def commit_changes(self, last_record: Optional[Base], model_class_name: str, changes_log: List[str]) -> None:
        """
        Commit the changes to the session and log them.

        :param last_record: The last record from the database.
        :param model_class_name: Formatted model class name for logging.
        :param changes_log: List of change logs.
        """
        self.session.commit()
        if not last_record:
            print(f"{self.ticker.symbol} - {model_class_name} - 1 record inserted.")
        else:
            for change in changes_log:
                print(change)

    def log_no_changes(self, model_class_name: str, print_no_changes: bool) -> None:
        """
        Log when no changes are detected.

        :param model_class_name: Formatted model class name for logging.
        :param print_no_changes: Whether to print a message when no changes are detected.
        """
        if print_no_changes:
            print(f"{self.ticker.symbol} - {model_class_name} - no changes detected")

    """ Generic bulk update handler for database operations. """
    def handle_generic_bulk_update(
            self,
            new_data_df: pd.DataFrame,
            model_class: Type[Base]
    ) -> None:
        """
        Handle the bulk update or insertion of records in the database by comparing with existing data.

        :param new_data_df: DataFrame containing the new data to be inserted or updated.
        :param model_class: The SQLAlchemy model class to interact with.
        :param db_columns: List of columns to retrieve from the database and compare with the new data.
        :param comparison_columns: List of columns to compare for detecting changes.
        """
        try:
            # __ prepare model class name for logging __
            model_class_name = self.format_model_class_name(model_class=model_class)

            # __ get comparison columns and non-nullable columns __
            comparison_columns = self.get_comparison_columns(model_class=model_class)
            non_nullable_columns = self.get_non_nullable_columns(model_class=model_class)

            # __ prepare new data __
            new_data_df = self.prepare_new_data(new_data_df=new_data_df, comparison_columns=comparison_columns, non_nullable_columns=non_nullable_columns)

            # __ read existing data from the database __
            existing_data_dict = self.read_existing_data(model_class, comparison_columns)

            # __ normalize comparison keys __
            normalize_value = self.get_normalize_value_function()

            # __ prepare the list for records to insert __
            records_to_insert = self.compare_and_prepare_inserts(
                new_data_df,
                existing_data_dict,
                model_class,
                comparison_columns,
                normalize_value
            )

            # __ perform the bulk insert __
            self.bulk_insert_records(records_to_insert=records_to_insert, model_class_name=model_class_name)

        except Exception as e:
            self.session.rollback()
            print(f"Error occurred during bulk update: {e}")

    @staticmethod
    def get_primary_keys_columns(model_class: Type[Base]) -> List[str]:
        """
        Get the primary key and non-nullable columns from the database table.

        :param model_class: The SQLAlchemy model class.
        :return: Tuple containing the primary key and non-nullable columns.
        """
        inspector = inspect(model_class)
        primary_keys = [pk.name for pk in inspector.columns if pk.primary_key]
        return [x for x in primary_keys if x not in ["ticker_id", "last_update"]]

    @staticmethod
    def get_non_nullable_columns(model_class: Type[Base]) -> List[str]:
        """
        Get the non-nullable columns from the database table.

        :param model_class: The SQLAlchemy model class.
        :return: List of non-nullable columns.
        """
        inspector = inspect(model_class)
        non_nullables = [c.name for c in inspector.columns if not c.nullable]
        return [x for x in non_nullables if x not in ["ticker_id", "last_update"]]

    def get_comparison_columns(self, model_class: Type[Base]) -> List[str]:
        """
        Get the columns to compare for detecting changes.

        :param model_class: The SQLAlchemy model class.
        :return: List of columns to compare for detecting changes.
        """
        primary_keys = self.get_primary_keys_columns(model_class)
        non_nullables = self.get_non_nullable_columns(model_class)
        return list(set(primary_keys + non_nullables))

    def prepare_new_data(self, new_data_df: pd.DataFrame, comparison_columns: list[str], non_nullable_columns: list[str]) -> pd.DataFrame:
        """
        Prepare the new data DataFrame by adding necessary columns and cleaning the data.

        :param new_data_df: DataFrame containing the new data.
        :param comparison_columns: List of columns to compare for detecting changes.
        :param non_nullable_columns: List of non-nullable columns.
        :return: Prepared DataFrame.
        """
        new_data_df['ticker_id'] = self.ticker.id
        new_data_df['last_update'] = datetime.now()

        # Convert empty strings to NaN for consistent handling of null values
        new_data_df = new_data_df.replace(r'^\s*$', None, regex=True)

        datetime_cols = new_data_df.select_dtypes(include=['datetime64']).columns
        new_data_df[datetime_cols] = new_data_df[datetime_cols].replace({pd.NaT: None})

        # Normalize column names to match SQLAlchemy model attributes
        new_data_df.columns = [col.lower().replace(' ', '_') for col in new_data_df.columns]

        # Drop rows with NaN values in the comparison columns
        new_data_df = new_data_df.dropna(subset=non_nullable_columns)

        # Drop duplicates based on the comparison columns
        new_data_df = new_data_df.drop_duplicates(subset=comparison_columns)

        return new_data_df

    def read_existing_data(self, model_class: Type[Base], comparison_columns: list[str]) -> dict:
        """
        Read and prepare the existing data from the database for comparison.

        :param model_class: The SQLAlchemy model class to interact with.
        :param comparison_columns: List of columns to compare for detecting changes.
        :return: Dictionary of existing data keyed by normalized comparison columns.
        """
        existing_data_query = self.session.query(model_class).filter(model_class.ticker_id == self.ticker.id).all()

        normalize_value = self.get_normalize_value_function()

        existing_data_dict = {
            tuple(normalize_value(getattr(record, col)) for col in comparison_columns): record
            for record in existing_data_query
        }

        return existing_data_dict

    @staticmethod
    def get_normalize_value_function() -> callable:
        """
        Get a function to normalize values for comparison.

        :return: Function to normalize values.
        """
        def normalize_value(value):
            if isinstance(value, Decimal):
                return float(value)
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, str) and value.strip() == "":
                return None
            return value

        return normalize_value

    def compare_and_prepare_inserts(
            self,
            new_data_df: pd.DataFrame,
            existing_data_dict: dict,
            model_class: Type[Base],
            comparison_columns: list[str],
            normalize_value: callable
    ) -> list:
        """
        Compare the new data with existing data and prepare records for insertion.

        :param new_data_df: DataFrame containing the new data.
        :param existing_data_dict: Dictionary of existing data keyed by normalized comparison columns.
        :param model_class: The SQLAlchemy model class to interact with.
        :param comparison_columns: List of columns to compare for detecting changes.
        :param normalize_value: Function to normalize values for comparison.
        :return: List of records to insert.
        """
        records_to_insert = []

        for _, row in new_data_df.iterrows():
            record_key = tuple(normalize_value(row[col]) for col in comparison_columns)
            new_record_data = row.to_dict()

            insert_new_record = True

            # Use keys_equal function to find a matching key in the existing data dictionary
            matching_key = next(
                (existing_key for existing_key in existing_data_dict.keys() if self.keys_equal(record_key, existing_key)),
                None
            )

            if matching_key:
                existing_record = existing_data_dict[matching_key]
                new_record_data.pop("last_update") if "last_update" in new_record_data else None
                changes_log = self.compare_and_log_changes(existing_record, new_record_data, model_class.__tablename__)

                if not changes_log:
                    insert_new_record = False

            if insert_new_record:
                new_record_data['ticker_id'] = self.ticker.id
                new_record_data['last_update'] = datetime.now()
                new_record = model_class(**new_record_data)
                records_to_insert.append(new_record)

        return records_to_insert

    @staticmethod
    def keys_equal(key1, key2) -> bool:
        """
        Check if two keys are equal, handling NaN and None values.

        :param key1: First key tuple.
        :param key2: Second key tuple.
        :return: True if keys are considered equal, otherwise False.
        """

        def are_equal(val1, val2):
            if (val1 is None and val2 is None) or (pd.isna(val1) and pd.isna(val2)):
                return True
            if isinstance(val1, float) and isinstance(val2, float):
                return abs(val1 - val2) < 1e-12
            return val1 == val2

        return all(are_equal(v1, v2) for v1, v2 in zip(key1, key2))

    def bulk_insert_records(self, records_to_insert: list, model_class_name: str) -> None:
        """
        Perform bulk insert of records into the database.

        :param records_to_insert: List of records to insert.
        :param model_class_name: Name of the model class for logging.
        """
        if records_to_insert:
            self.session.bulk_save_objects(records_to_insert)
            self.session.commit()
            print(f"{self.ticker.symbol} - {model_class_name} - {len(records_to_insert)} records inserted.")
        # else:
        #     print(f"{self.ticker.symbol} - {model_class_name} - no changes detected")

    def bulk_insert(self, data: pd.DataFrame, model_class: Type[Base]) -> None:
        """
        Bulk insert the given data into the database.

        :param data: DataFrame containing the data to be inserted.
        :param model_class: The SQLAlchemy model class to interact with.
        """
        # Convert the DataFrame to a list of dictionaries
        data_dicts = data.to_dict(orient='records')

        # Convert these dictionaries into SQLAlchemy model instances
        instances = [model_class(**record) for record in data_dicts]

        # Add all instances to the session and commit
        self.session.bulk_save_objects(instances)
        self.session.commit()
