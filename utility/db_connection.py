from sqlalchemy import create_engine # For creating engine
import sys
import os
import threading # mutlti thread process

class DatabaseConnection:

    # Creating a thread inialization
    def __init__(self):
        self.LocalThread = threading.local()

    def get_database(self, db_str_conn):
        conn_pool = getattr(self.LocalThread, 'conn_pool', {})
        con = db_str_conn
        try:
            if con in conn_pool and conn_pool[con] is not None:
                print("Existing/Previous Database connection id is : %s"% (id(conn_pool[con])))
                return conn_pool[con]
            else:
                r_conn = self.db_conn_reset(db_str_conn)
                return r_conn
        except Exception as e:
            Type_of_Object = sys.exc_info()
            Type_of_Exception = Type_of_Object[0]
            tb_of_Exception = Type_of_Object[2]
            Name_of_Fun = os.path.split(tb_of_Exception.tb_frame.f_code.co_filename)[1]
            exception_string = str(Type_of_Exception) + " & " + str(Name_of_Fun) + " & " + str(tb_of_Exception.tb_lineno)
            print("An exception has occurred in the DB connection over-all as : %s, at : %s"% (e, exception_string))

    def db_conn_reset(self, db_str_conn):
        conn = None
        con_key = db_str_conn
        conn_pool = getattr(self.LocalThread, 'conn_pool', {})
        try:
            if con_key in conn_pool and conn_pool[con_key] is not None:
                conn_pool[con_key].dispose()
        except Exception as e:
            Type_of_Object = sys.exc_info()
            Type_of_Exception = Type_of_Object[0]
            tb_of_Exception = Type_of_Object[2]
            Name_of_Fun = os.path.split(tb_of_Exception.tb_frame.f_code.co_filename)[1]
            exception_string = str(Type_of_Exception) + " & " + str(Name_of_Fun) + " & " + str(tb_of_Exception.tb_lineno)
            print("An exception has occurred in the DB connection as : %s, at : %s"% (e, exception_string))

        for i in range(0,3):
            try:
                conn = create_engine(db_str_conn)
                break
            except Exception as e:
                Type_of_Object = sys.exc_info()
                Type_of_Exception = Type_of_Object[0]
                tb_of_Exception = Type_of_Object[2]
                Name_of_Fun = os.path.split(tb_of_Exception.tb_frame.f_code.co_filename)[1]
                exception_string = str(Type_of_Exception) + " & " + str(Name_of_Fun) + " & " + str(tb_of_Exception.tb_lineno)
                print("An exception has occurred in the DB connection as : %s, at : %s"% (e, exception_string))
                conn = None
                continue
        if conn is not None:
            print("A new database connection created with the id is : %s"% (id(conn)))
            conn_pool[con_key] = conn
            self.LocalThread.conn_pool = conn_pool
        return conn

DB = DatabaseConnection()

