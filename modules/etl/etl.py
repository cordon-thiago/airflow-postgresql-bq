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

    def pg_load_from_csv_file(self, csv_source_file, pg_str_conn, pg_schema, pg_dest_table, csv_header = True):
        
        # Create table syntax
        query_create_table = """
        create table public.hardbounce_raw
        (
            emailDomain_cat varchar(255)
            ,emailDomainPiece1 varchar(255)
            ,emailDomainPiece2 varchar(255)
            ,regDate_n date
            ,birthDate_n date
            ,monthsSinceRegDate int
            ,age int
            ,percNumbersInEmailUser numeric(10,2)
            ,hasNumberInEmailUser int
            ,emailUserCharQty int
            ,flgHardBounce_n int
        );
        """

        # Create a pg connection object
        pg_conn = self.__pg_connection(pg_str_conn)

        # Check if table exists
        print("Result check table exists: ", self.__pg_check_table_exists(pg_conn, pg_schema, pg_dest_table))

        # Create table if not exists
        if not self.__pg_check_table_exists(pg_conn, pg_schema, pg_dest_table):
            self.__pg_create_table(pg_conn, query_create_table)

        # Create pg cursor
        #pg_cursor = pg_conn.cursor()

        # Read CSV File
        #with open(csv_source_file, 'r') as f:
        #    reader = csv.reader(f)
        #    if csv_header:
        #        next(reader) # skip header row
            # Loop each line in CSV
        #    for row in reader:





        # Initialize PG Connection using PostgresHook

        # Insert Rows into PG Table

        # Test if # rows in csv equal to # rows in PG Table

        # Close PG connection
