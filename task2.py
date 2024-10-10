from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine
from tqdm import tqdm

class Task_2_Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def show_tables(self):
        print("Showing tables:")
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def count_users_activities_trackpoints(self):
        self.cursor.execute("SELECT COUNT(*) FROM User")
        user_count = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT COUNT(*) FROM Activity")
        activity_count = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT COUNT(*) FROM TrackPoint")
        trackpoint_count = self.cursor.fetchone()[0]

        return user_count, activity_count, trackpoint_count
    
    def avg_activities_per_user(self):
        self.cursor.execute("SELECT COUNT(*) FROM User")
        user_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM Activity")
        activity_count = self.cursor.fetchone()[0]
        
        avg_activities = activity_count / user_count

        return round(avg_activities,3)
    
    def top_20_users_with_most_activities(self):
        query = """
                SELECT user_id, COUNT(*) AS activity_count 
                FROM Activity 
                GROUP BY user_id 
                ORDER BY activity_count DESC LIMIT 20
        """
        self.cursor.execute(query)
        top_20_users = self.cursor.fetchall()
        return top_20_users
    
    def find_taxi_users(self):
        query = """
                SELECT DISTINCT user_id 
                FROM Activity 
                WHERE transportation_mode = 'taxi'
        """
        self.cursor.execute(query)
        users = self.cursor.fetchall()
        
        if users:
            print("Users who have taken a taxi:")
            for user in users:
                print(user[0])
        else:
            print("No users have taken a taxi.")

    def count_transportation_modes(self):
        query = """
        SELECT transportation_mode, COUNT(*) AS mode_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        ORDER BY mode_count DESC
        """
        self.cursor.execute(query)
        modes = self.cursor.fetchall()

        if modes:
            print("Transportation modes and their activity counts (sorted by count):")
            for mode, count in modes:
                print(f"{mode}: {count}")
        else:
            print("No transportation modes found.")

    def find_year_with_most_activities_and_hours(self):
        query_activities = """
        SELECT YEAR(start_date_time) AS activity_year, COUNT(*) AS activity_count
        FROM Activity
        GROUP BY activity_year
        ORDER BY activity_count DESC
        LIMIT 1
        """
        self.cursor.execute(query_activities)
        most_activities_year = self.cursor.fetchone()

        query_hours = """
        SELECT YEAR(start_date_time) AS hour_year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
        FROM Activity
        GROUP BY hour_year
        ORDER BY total_hours DESC
        LIMIT 1
        """
        self.cursor.execute(query_hours)
        most_hours_year = self.cursor.fetchone()

        if most_activities_year and most_hours_year:
            activity_year, activity_count = most_activities_year
            hours_year, total_hours = most_hours_year

            print(f"Year with most activities: {activity_year} (Activities: {activity_count})")
            print(f"Year with most recorded hours: {hours_year} (Total Hours: {total_hours})")

            if activity_year == hours_year:
                print(f"Yes, {activity_year} is also the year with the most recorded hours.")
            else:
                print(f"No, the year with the most recorded hours is {hours_year}.")
        else:
            print("No activities found.")


    def total_distance_walked_by_user_in_2008(self, user_id=112):
        query = """
        SELECT lat, lon, date_time, activity_id
        FROM TrackPoint tp
        JOIN Activity a ON tp.activity_id = a.id
        JOIN User u ON a.user_id = u.id
        WHERE YEAR(tp.date_time) = 2008 AND u.id = %s AND a.transportation_mode = "WALK"
        ORDER BY tp.date_time
        """
        
        self.cursor.execute(query, (user_id,))
        trackpoints = self.cursor.fetchall()

        total_distance = 0.0

        activity_distance = 0.0

        # Calculate the distance between consecutive trackpoints
        for i in range(1, len(trackpoints)):
            lat1, lon1, _, activity_id1 = trackpoints[i - 1]  # previous point
            lat2, lon2, _, activity_id2 = trackpoints[i]      # current point
            if activity_id1 != activity_id2:
                total_distance += activity_distance
                activity_distance = 0
            else:
                activity_distance += haversine((lat1, lon1), (lat2, lon2))
                
        total_distance += activity_distance

        if total_distance > 0:
            print(f"Total distance walked by user {user_id} in 2008: {total_distance:.2f} km")
        else:
                print(f"No data found for user {user_id} in 2008.")

    def top_20_users_by_altitude_gain(self):
            query = """
            SELECT u.id AS user_id, tp.altitude, tp.date_time
            FROM TrackPoint tp
            JOIN Activity a ON tp.activity_id = a.id
            JOIN User u ON a.user_id = u.id
            WHERE tp.altitude != -777 -- Exclude invalid altitude
            ORDER BY u.id, tp.date_time;
            """
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            user_altitude_gain = {}

            # Use tqdm to display progress
            for i in tqdm(range(1, len(results)), desc="Processing altitude gains", unit="trackpoints"):
                current_row = results[i]
                previous_row = results[i - 1]

                current_user = current_row[0]
                current_altitude = current_row[1]
                previous_user = previous_row[0]
                previous_altitude = previous_row[1]

                # If it's the same user and altitude is increasing, compute the gain
                if current_user == previous_user and current_altitude > previous_altitude:
                    altitude_gain = (current_altitude - previous_altitude) * 0.3048  # Convert feet to meters
                    if current_user in user_altitude_gain:
                        user_altitude_gain[current_user] += altitude_gain
                    else:
                        user_altitude_gain[current_user] = altitude_gain

            # Sort the users by total altitude gain and take the top 20
            top_20_users = sorted(user_altitude_gain.items(), key=lambda x: x[1], reverse=True)[:20]

            if top_20_users:
                print("Top 20 Users by Altitude Gain (User ID, Total Meters Gained):")
                print(tabulate(top_20_users, headers=["User ID", "Total Meters Gained"], tablefmt="grid"))
            else:
                print("No altitude data found.")
                    
    def find_users_with_invalid_activities(self):
        query = """
        SELECT u.id AS user_id, a.id AS activity_id, tp.date_time
        FROM User u
        JOIN Activity a ON u.id = a.user_id
        JOIN TrackPoint tp ON a.id = tp.activity_id
        ORDER BY u.id, a.id, tp.date_time;
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        invalid_activities_per_user = {}
        
        total_results = len(results)  
        loading_step = max(total_results // 20, 1)  # Update every 5%
        print("Processing trackpoints...")

        # Iterate through the results to find invalid activities
        current_activity_id = None
        previous_trackpoint_time = None
        current_user_id = None
        invalid_activity_found = False
        processed_count = 0

        for row in results:
            user_id, activity_id, trackpoint_time = row

            # Check if we are processing a new activity
            if activity_id != current_activity_id:
                # If an invalid activity was found, increment the count for the user
                if invalid_activity_found:
                    if current_user_id not in invalid_activities_per_user:
                        invalid_activities_per_user[current_user_id] = 0
                    invalid_activities_per_user[current_user_id] += 1

                # Reset tracking variables for the new activity
                current_activity_id = activity_id
                previous_trackpoint_time = trackpoint_time
                current_user_id = user_id
                invalid_activity_found = False
            else:
                # Calculate time difference between consecutive trackpoints
                time_diff = (trackpoint_time - previous_trackpoint_time).total_seconds() / 60.0  # convert to minutes
                
                if time_diff >= 5:
                    invalid_activity_found = True
                
                previous_trackpoint_time = trackpoint_time

            processed_count += 1
            if processed_count % loading_step == 0:
                print(f"Processed {processed_count} of {total_results} trackpoints...")

        # Final check for the last activity
        if invalid_activity_found:
            if current_user_id not in invalid_activities_per_user:
                invalid_activities_per_user[current_user_id] = 0
            invalid_activities_per_user[current_user_id] += 1

        if invalid_activities_per_user:
            print("Users with Invalid Activities (User ID, Invalid Activity Count):")
            for user_id, count in sorted(invalid_activities_per_user.items(), key=lambda x: x[1], reverse=True):
                print(f"User {user_id}: {count} invalid activities")
        else:
            print("No users with invalid activities found.")


    def find_users_in_forbidden_city(self):
        # Exact coordinates for the Forbidden City
        forbidden_city_lat = 39.916
        forbidden_city_lon = 116.397
        tolerance = 0.001  # Adjust tolerance as necessary

        query = """
        SELECT DISTINCT u.id
        FROM User u
        JOIN Activity a ON u.id = a.user_id
        JOIN TrackPoint tp ON a.id = tp.activity_id
        WHERE tp.lat BETWEEN %s AND %s
        AND tp.lon BETWEEN %s AND %s;
        """
        
        self.cursor.execute(query, 
                            (forbidden_city_lat, 
                            forbidden_city_lat + tolerance, 
                            forbidden_city_lon, 
                            forbidden_city_lon + tolerance))
        
        results = self.cursor.fetchall()
        
        if results:
            print("Users who have tracked an activity in the Forbidden City (with tolerance):")
            for row in results:
                print(f"User ID: {row[0]}")
        else:
            print("No users found who have tracked an activity in the Forbidden City.")
                
    def get_most_used_transportation_mode(self):
        print("Finding users with their most used transportation mode...")
        
        query = """
        SELECT user_id, transportation_mode, COUNT(*) as mode_count
        FROM Activity
        WHERE transportation_mode != 'unknown'
        GROUP BY user_id, transportation_mode
        ORDER BY user_id, mode_count DESC
        """
        
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        most_used_modes = {}
        
        for row in rows:
            user_id = row[0]
            transportation_mode = row[1]
            mode_count = row[2]
            
            if user_id not in most_used_modes:
                most_used_modes[user_id] = (transportation_mode, mode_count)
            else:
                if mode_count > most_used_modes[user_id][1]:
                    most_used_modes[user_id] = (transportation_mode, mode_count)
        
        print("\nMost used transportation modes per user:")
        for user_id, (mode, _) in sorted(most_used_modes.items()):
            print(f"User ID: {user_id}, Most Used Mode: {mode}")


def main():
    program = None
    try:
        program = Task_2_Program()
           
        program.show_tables()
        user_count, activity_count, trackpoint_count = program.count_users_activities_trackpoints()
        print("Number of users:", user_count)
        print("Number of activities:", activity_count)
        print("Number of trackpoints:", trackpoint_count)
        avg_activities = program.avg_activities_per_user()
        print("Average number of activities per user:", avg_activities)
        top_20_users = program.top_20_users_with_most_activities()
        print("Top 20 users with most activities:")
        print(tabulate(top_20_users, headers=["User ID", "Activity count"]))
        program.find_taxi_users()
        program.count_transportation_modes()
        program.find_year_with_most_activities_and_hours()
        program.total_distance_walked_by_user_in_2008()
        program.find_users_with_invalid_activities()
        program.find_users_in_forbidden_city()
        program.top_20_users_by_altitude_gain()
        program.get_most_used_transportation_mode()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
