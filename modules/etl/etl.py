from airflow.hooks.postgres_hook import PostgresHook
import csv

class Etl:

    def pg_check_table_exists(self, pg_connection_id, schema, table):
        
        # Define query to check if a table exists
        query = """
            SELECT max(1) as column FROM information_schema.tables
            WHERE table_schema = '{}'
            AND table_name = '{}';
        """.format(schema, table)

        # Create a connection object
        pg_hook_conn = PostgresHook(postgres_conn_id=pg_connection_id)

        # Execute query
        query_results = pg_hook_conn.get_records(sql=query)

        # Check results
        if query_results[0][0] == 1:
            return True            
        else:
            return False

    def pg_load_from_csv_file(self, csv_source_file, pg_connection_id, pg_schema, pg_dest_table, csv_header = True):
        
        # Read CSV File
        with open(csv_source_file, 'r') as f:
            reader = csv.reader(f)
            if csv_header:
                next(reader) # skip header row
            # Loop each line in CSV
            # for row in reader:



        # Initialize PG Connection using PostgresHook

        # Insert Rows into PG Table

        # Test if # rows in csv equal to # rows in PG Table

        # Close PG connection
