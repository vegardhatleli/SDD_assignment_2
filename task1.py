from DbConnector import DbConnector
from tabulate import tabulate
import os


class Task_1_Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS User (
            user_id VARCHAR(255) PRIMARY KEY,  -- Assuming user IDs are strings
            is_labeled BOOLEAN                 -- Column to indicate if the user is labeled
        );
        """
        self.cursor.execute(create_table_query)
        self.db_connection.commit()

    def create_activites_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Activity (
        id INT PRIMARY KEY AUTO_INCREMENT,
        user_id VARCHAR(255),
        transportation_mode VARCHAR(255),
        start_date_time DATETIME,
        end_date_time DATETIME,
        FOREIGN KEY (user_id) REFERENCES User(user_id)
);  """
        self.cursor.execute(create_table_query)
        self.db_connection.commit()
        


    def insert_users(self, base_dir, labeled_ids_file):
        # Read labeled IDs from labeled_ids.txt
        with open(labeled_ids_file, 'r') as f:
            labeled_ids = {line.strip() for line in f}  # Using a set for fast lookups
        
        for user_folder in os.listdir(base_dir):
            user_path = os.path.join(base_dir, user_folder)
            
            # Check if it's a directory (user folder) and not a file
            if os.path.isdir(user_path):
                user_id = user_folder  # Assuming folder name is the user ID
                
                # Check if the user ID is in the labeled_ids set
                is_labeled = user_id in labeled_ids
                
                # Insert user into the USER table
                query = """
                INSERT INTO User (user_id, is_labeled)
                VALUES (%s, %s)
                """
                self.cursor.execute(query, (user_id, is_labeled))
                self.db_connection.commit()

    def insert_activities(self, base_dir):
        for root, dirs, files in os.walk(base_dir):
            # Iterate through subdirectories
            for file in files:
                file_path = os.path.join(root, file)  # Get the full path
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                    line_count = len(lines)
                    
                    # Process file if it has 2500 or fewer data lines (excluding the first 6 lines)
                    if line_count - 6 <= 2500:
                        start_time = lines[6].strip().split(',')[6]
                        start_date = lines[6].strip().split(',')[5]
                        end_time = lines[-1].strip().split(',')[6]
                        end_date = lines[-1].strip().split(',')[5]
                        start_date_time = start_date + ' ' + start_time
                        end_date_time = end_date + ' ' + end_time

                        #TODO Skriv sprørring med riktige verdier inn i Activity! Foreign key ????
                        print(start_date_time)
                        print(end_date_time)
                        query = """
                        INSERT INTO Activity (start_date_time, end_date_time)
                        VALUES (%s, %s)
                        """
                        


    def insert_trackpoints(self, base_dir):
        for root, dirs, files in os.walk(base_dir):
            # Iterate through subdirectories
            for dir_name in dirs:
                trajectory_dir = os.path.join(root, dir_name, 'Trajectory')
                
            
                # Get all files inside 'Trajectory' folder
                for file_name in os.listdir(trajectory_dir):
                    file_path = os.path.join(trajectory_dir, file_name)
                    print(file_path)
                        
                    with open(file_path, 'r') as file:
                        lines = file.readlines()
                        line_count = len(lines)
                        
                        # Process file if it has 2500 or fewer data lines (excluding the first 6 lines)
                        if line_count - 6 <= 2500:
                            for line in lines[6:]:
                                data = line.strip().split(',')
                                latitude = data[0]
                                longitude = data[1]
                                altitude = data[3]
                                timestamp = data[4]
                                
                                query = """
                                INSERT INTO User (latitude, longitude, altitude, timestamp)
                                VALUES (%s, %s, %s, %s)
                                """
                                self.cursor.execute(query, (latitude, longitude, altitude, timestamp))
                                self.db_connection.commit()


    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = Task_1_Program()

        #program.drop_table("Activity")
        #program.drop_table("User")
        #program.create_user_table()
        #program.insert_users(base_dir="dataset/dataset/Data",labeled_ids_file="dataset/dataset/labeled_ids.txt")
        #program.create_activites_table()
        program.insert_activities(base_dir="dataset/dataset/Data")
        #program.insert_data(base_dir="dataset/dataset/Data")
        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
