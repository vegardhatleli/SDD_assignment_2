from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime


class Task_1_Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(255) PRIMARY KEY,  -- Assuming user IDs are strings
            is_labeled BOOLEAN                 -- Column to indicate if the user is labeled
        );
        """
        self.cursor.execute(create_table_query)
        self.db_connection.commit()

    def create_activites_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Activity (
        id VARCHAR(255) PRIMARY KEY,
        user_id VARCHAR(255),
        transportation_mode VARCHAR(255),
        start_date_time DATETIME,
        end_date_time DATETIME,
        FOREIGN KEY (user_id) REFERENCES User(id)
);  """
        self.cursor.execute(create_table_query)
        self.db_connection.commit()

    def create_trackpoints_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
        id int AUTO_INCREMENT NOT NULL PRIMARY KEY,
        activity_id VARCHAR(255),
        lat double,
        lon double,
        altitude int,
        date_days double,
        date_time datetime,
        FOREIGN KEY (activity_id) REFERENCES Activity(id)
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
                INSERT INTO User (id, is_labeled)
                VALUES (%s, %s)
                """
                self.cursor.execute(query, (user_id, is_labeled))
                self.db_connection.commit()


    def insert_activities(self, base_dir):
        for root, dirs, files in os.walk(base_dir):
            # Check if 'Trajectory' folder exists in the current root
            if 'Trajectory' in dirs:
                # Extract the UserID from the current folder (which contains 'Trajectory' and 'labels.txt')
                user_id = os.path.basename(root)
                
                labels = {}
                labels_file_path = os.path.join(root, 'labels.txt')
                
                # Check if 'labels.txt' exists in the current folder and parse it
                if os.path.exists(labels_file_path):
                    with open(labels_file_path, 'r') as f:
                        f.readline()  # Skip header line
                        for line in f:
                            start_time_str, end_time_str, transportation_mode = line.strip().split('\t')
                            start_time = datetime.strptime(start_time_str, "%Y/%m/%d %H:%M:%S")
                            end_time = datetime.strptime(end_time_str, "%Y/%m/%d %H:%M:%S")
                            labels[(start_time, end_time)] = transportation_mode
                
                # Now move into the 'Trajectory' folder to process the .plt files
                trajectory_folder = os.path.join(root, 'Trajectory')
                for plt_file in os.listdir(trajectory_folder):
                    if plt_file.endswith(".plt"):
                        file_path = os.path.join(trajectory_folder, plt_file)  # Get the full path of the .plt file
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
                                start_date_time = datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S")
                                end_date_time = datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S")

                                # Default transportation mode if no label found
                                transportation_mode = "unknown"

                                # Check if there is a matching label in the labels dictionary
                                for (label_start, label_end), mode in labels.items():
                                    # Check if the time range of the plt file overlaps with the time range in labels
                                    if label_start == start_date_time and label_end == end_date_time:
                                        transportation_mode = mode
                                        break
                                #TODO: fix into int in db 
                                id = int(plt_file.split('.')[0] + user_id)
                                # Insert query with UserID, start/end times, and transportation mode
                                query = """
                                INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time)
                                VALUES (%s, %s, %s, %s, %s)
                                """
                                data = (id, user_id, transportation_mode, start_date_time, end_date_time)

                                self.cursor.execute(query, data)
                                self.db_connection.commit()

    def insert_trackpoints(self, base_dir):
        batch_data = []
        batch_size = 10000  # You can adjust the batch size based on your requirements
        file_count = 0

        for root, dirs, files in os.walk(base_dir):
            # Check if 'Trajectory' folder exists in the current root
            if 'Trajectory' in dirs:
                # Extract the UserID from the current folder (which contains 'Trajectory' and 'labels.txt')
                user_id = os.path.basename(root)
                trajectory_folder = os.path.join(root, 'Trajectory')

                # Get all files inside 'Trajectory' folder
                for file_name in os.listdir(trajectory_folder):
                    file_path = os.path.join(trajectory_folder, file_name)
                    activity_id = int(file_name.split('.')[0] + user_id)

                    with open(file_path, 'r') as file:
                        lines = file.readlines()
                        line_count = len(lines)

                        # Process file if it has 2500 or fewer data lines (excluding the first 6 lines)
                        if line_count - 6 <= 2500:
                            print(f"Processing file: {file_name} for user: {user_id}")
                            for line in lines[6:]:
                                data = line.strip().split(',')
                                latitude = data[0]
                                longitude = data[1]
                                altitude = data[3]
                                date_days = data[4]
                                date_time = data[5] + ' ' + data[6]
                                date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                                
                                if altitude != '-777':

                                # Add data to the batch list
                                    batch_data.append((activity_id, latitude, longitude, altitude, date_days, date_time))

                                # If the batch size is reached, insert the batch and clear the list
                                if len(batch_data) >= batch_size:
                                    print(f"Inserting batch of {len(batch_data)} trackpoints...")
                                    self.insert_batch(batch_data)
                                    batch_data.clear()

                            file_count += 1

        # Insert any remaining data
        if batch_data:
            print(f"Inserting final batch of {len(batch_data)} trackpoints...")
            self.insert_batch(batch_data)

        print(f"Finished processing {file_count} files.")

    def insert_batch(self, batch_data):
        query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.cursor.executemany(query, batch_data)
        self.db_connection.commit()
        print(f"Inserted {len(batch_data)} trackpoints.")


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
        #program.insert_activities(base_dir="dataset/dataset/Data")
        #program.drop_table("TrackPoint")
        program.create_trackpoints_table()
        program.insert_trackpoints(base_dir="dataset/dataset/Data")
        program.show_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
