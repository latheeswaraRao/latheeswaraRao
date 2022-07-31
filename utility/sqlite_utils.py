from utility.db_connection import DB

class SQLLiteUtils:

    def __init__(self, db_str_conn):
        self.connection = DB.get_database(db_str_conn=db_str_conn)

    # Creating a function for pushing datasest
    def put_data(self, df, table_name, db_str_conn):
        for val in range(0, 3):
            try:
                df.to_sql(name=table_name, con=self.connection, if_exists='replace', index=False)
                break
            except Exception as e:
                print("An %s"% (e), " exception has occurred")
                self.connection = DB.db_conn_reset(db_str_conn=db_str_conn)
                continue


