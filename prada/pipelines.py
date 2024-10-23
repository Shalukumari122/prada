# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from prada.items import PraDetailsItem, PralinkItem


class PradaPipeline:

    def __init__(self):
        # Initialize the pipeline and connect to MySQL database
        self.conn = pymysql.connect(
            host='localhost',         # Database host
            user='root',              # Database user
            password='actowiz',       # Database password
            database='prada_db'     # Database name
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        details_data_table = 'details_data'  # Table for storing detailed item data
        link_table='link'

        if isinstance(item, PraDetailsItem):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                   CREATE TABLE IF NOT EXISTS {details_data_table} (
                                       `Store No.` INT AUTO_INCREMENT PRIMARY KEY,

                                       {columns_definitions}
                                   )
                               """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM {details_data_table}")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE {details_data_table} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  IGNORE INTO {details_data_table} ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")

            try:
                # Update `subcat_of_cat_link` status
                if 'unique_id' in item:
                    update_query = "UPDATE link SET status = 'Done' WHERE unique_id = %s"
                    self.cursor.execute(update_query, (item['unique_id'],))
                    self.conn.commit()
                else:
                    print("unique_id not found in item.")
            except Exception as e:
                print(f"Error updating link: {e}")

        if isinstance(item, PralinkItem):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                   CREATE TABLE IF NOT EXISTS {link_table} (
                                       `Store No.` INT AUTO_INCREMENT PRIMARY KEY,

                                       {columns_definitions}
                                   )
                               """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM {link_table}")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE {link_table} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO {link_table} ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")
        return item
