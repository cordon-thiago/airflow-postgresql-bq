import psycopg2
import csv
class Etl:

    def __pg_connection(self, pg_str_conn):
        
        # Create a connection object
        pg_conn = psycopg2.connect(pg_str_conn)

        return pg_conn

    def __pg_check_table_exists(self, pg_conn, schema, table):
        
        # Define query to check if a table exists
        query = """
            SELECT max(1) as column FROM information_schema.tables
            WHERE table_schema = '{}'
            AND table_name = '{}';
        """.format(schema, table)

        # create cursor
        pg_cursor = pg_conn.cursor()

        # Execute query
        pg_cursor.execute(query)
        query_results = pg_cursor.fetchall()

        # Check results
        if query_results[0][0] == 1:
            return True            
        else:
            return False

    def __pg_create_table(self, pg_conn, create_syntax):

        # create cursor
        pg_cursor = pg_conn.cursor()

        # Create table
        try:
            pg_cursor.execute(create_syntax)
            pg_conn.commit()
            print("Table successfully created!")
        except Exception as e:
            print("Error creating table: ", e)

    def pg_load_from_csv_file(self, csv_source_file, file_delimiter, pg_str_conn, pg_schema, pg_dest_table, csv_header = True):
        
        # Create table syntax
        query_create_table = """
        create table {}.{}
        (
            emailDomain_cat varchar(255)
            ,emailDomainPiece1 varchar(255)
            ,emailDomainPiece2 varchar(255)
            ,regDate_n varchar(255)
            ,birthDate_n varchar(255)
            ,monthsSinceRegDate varchar(255)
            ,age varchar(255)
            ,percNumbersInEmailUser varchar(255)
            ,hasNumberInEmailUser varchar(255)
            ,emailUserCharQty varchar(255)
            ,flgHardBounce_n varchar(255)
        );
        """.format(pg_schema, pg_dest_table)

        # Create a pg connection object
        pg_conn = self.__pg_connection(pg_str_conn)

        # Check if table exists
        print("Result check table exists: ", self.__pg_check_table_exists(pg_conn, pg_schema, pg_dest_table))

        # Create table if not exists
        if not self.__pg_check_table_exists(pg_conn, pg_schema, pg_dest_table):
            self.__pg_create_table(pg_conn, query_create_table)

        # Create pg cursor
        pg_cursor = pg_conn.cursor()

        # Read CSV File
        with open(csv_source_file, 'r') as f:

            if csv_header:
                next(f) # skip header row
            try:
                # Copy data to table
                pg_cursor.copy_from(f, pg_dest_table, sep=file_delimiter)
                pg_conn.commit()
                print("Rows successfully inserted!")
            except Exception as e:
                print("Error inserting data: ", e)
        
        # Close connection
        pg_conn.close()

        # Initialize PG Connection --ok

        # Insert Rows into PG Table --ok

        # Test if # rows in csv equal to # rows in PG Table

        # Close PG connection
