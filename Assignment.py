import sys,os,random,math

import numpy as np
import pandas as pd
import sqlalchemy as db

NAME_OF_DATABASE = "sqlite:///data.db"
PATH_OF_DATABASE = "data.db"

class ExceptionForNotEnoughArguments(Exception):
    '''
    Raises an exception when not enough arguments provided
    '''
    pass

class Ideal_Data_PointArgumentsException(Exception):
    '''
    Raises an exception when Ideal_Data_Point doesn't have required arguments
    '''
    pass

class Node_Function():
    '''
    Abstract class. Represents function with x and y data
    '''
    def __init__(self, x, y, **kwargs) -> None:
        '''
        Parameter x: x value of a function
        Parameter y: y value of a function
        '''
        self.x = float(x)
        self.y = float(y)

class Node_Test(Node_Function):
    '''
    Represents test function with x and y values.
    '''
    pass

class Ideal_Data_Point(Node_Function):
    '''
    Represents ideal function with x, y values and its number
    '''
    def __init__(self, x, y, **kwargs) -> None:
        '''
        Parameter x: x value of a function
        Parameter y: y value of a function
        Parameter number: number of a function
        Parameter deviation: max deviation between training function and ideal function
        '''
        super().__init__(x, y, **kwargs)
        self.number = kwargs.get("number")
        if self.number is None:
            raise Ideal_Data_PointArgumentsException("Missing required argument <number>")
        self.deviation = kwargs.get("deviation")
        if self.deviation is None:
            raise Ideal_Data_PointArgumentsException("Missing required argument <deviation>")


class Creating_Database():
    '''
    Creates database and tables, performs queries 
    '''
    def __init__(self, train_path, ideal_path):
        '''
        Parameter train_path: path to a train csv file
        Parameter ideal_path: path to a ideal csv file
        '''

        # Creating sqllite DB
        print("Creating database and tables...")
        self.engine = db.create_engine(NAME_OF_DATABASE, echo=False)

        # Creating tables for ideal and training functions
        self.connection = self.engine.connect()
        self.create_table("train", path=train_path)
        self.create_table("ideal", path=ideal_path)

        # Creating table for final results
        columns = (
            db.Column('x', db.Float, primary_key=True),
            db.Column('y', db.Float),
            db.Column('delta_y', db.Float),
            db.Column('ideal_number', db.Integer)
        )
        self.results = self.create_table("results", columns=columns)

        # Connect to tables
        meta = db.MetaData()
        self.train = db.Table("train", meta, autoload=True, autoload_with=self.engine)
        self.ideal = db.Table("ideal", meta, autoload=True, autoload_with=self.engine)
        

    def create_table(self, name, path=None, columns=None):
        '''
        Creates table either from csv file or with columns

        Parameter name: table name
        Parameter path: if path is specified, creates table from csv file
        Parameter columns: list of Column objects. If columns specified, creates table with those columns
        :raises ExceptionForNotEnoughArguments: if both path and columns are missing
        '''

        if path:
            with open(path, 'r') as f:
                data_df = pd.read_csv(f)
            data_df.to_sql(name, con=self.engine, index=False)
            return
        if columns:
            meta = db.MetaData()
            table = db.Table(
                name, meta, *columns
            )
            meta.create_all(self.engine)
            return table
        # if no path or columns
        raise ExceptionForNotEnoughArguments("Requesting to provide either path or columns")
    
    def retrieve_train_data(self, x):
        '''
        Queries training table, returns row with specified x
        Parameter x: x value
        :returns: returns list with value from row where x == <x>
        '''

        connection = self.engine.connect()
        query = db.select([self.train]).where(self.train.columns.x == x)
        return connection.execute(query).first() 
    
    def retrieve_ideal_data(self, x):
        '''
        Queries ideal table, returns row with specified x
        Parameter x: x value
        :returns: returns list with value from row where x == <x>
        '''
        
        connection = self.engine.connect()
        query = db.select([self.ideal]).where(self.ideal.columns.x == x)
        return connection.execute(query).first()
    
    def add_record(self, table, values):
        '''
        Adds record to specified table

        Parameter table: table object
        Parameter values: list of values
        '''

        connection = self.engine.connect()
        connection.execute(table.insert(), values)


class Matching_Functions():
    '''
    Performs matching between test data and ideal functions
    '''
    def __init__(self, database, test_data):
        """
        Parameter database: Database class instance
        Parameter test_data: path to test data csv file
        """

        self.database = database
        self.test = test_data
        self.mapping = {}
    
    def Finding_squares_deviation(self, numbers):
        '''
        Parameter numbers: array like object with numbers
        :returns: maximum squared deviation between numbers
        '''

        average = sum(numbers, 0.0) / len(numbers)
        deviation = [abs(i - average)**2 for i in numbers]
        return sum(deviation)
    
    def get_dev(self, x, y):
        '''
        get deviation between two value
        Parameter x: value1
        Parameter y: value2
        :returns: deviation as float
        '''
        return abs(float(x)-float(y))

    def process_test_data(self):
        '''
        Processes csv file line by line
        '''
        insert_data = []
        with open(self.test) as f:
            for line in f.readlines()[1:]:
                x, y = line.rstrip().split(',')
                test = Node_Test(x, y)

                # Get ideal functions
                ideal_functions = self.find_match_train(test)

                # Choose one or None matching ideal functions
                ideal = self.get_ideal_for_test(test, ideal_functions)
                self.mapping[test] = ideal # Can be Ideal_Data_Point or None
                if ideal:
                    insert_data.append({"x": test.x, "y": test.y, "delta_y": ideal.y, "ideal_number": ideal.number})
        self.database.add_record(self.database.results, insert_data)

    def find_match_train(self, test):
        '''
        Finds matching training and ideal functions
        Parameter test: Node_Test
        :returns: list of 4 Ideal_Data_Point objects
        '''
        # Get corresponding train and ideal functions
        train_ys = self.database.retrieve_train_data(test.x)[1:]
        ideal_ys = self.database.retrieve_ideal_data(test.x)[1:]

        # Choose 4 ideal function (one for each training function)
        ideal = [] 
        for y in train_ys:
            ideal.append(self.choose_ideal(test.x, y, ideal_ys))
        return ideal
        
    def choose_ideal(self, x, train_y, ideal_ys):
        '''
        Chooses ideal function for training function
        Parameter x: x value
        Parameter y: training function y value
        Parameter ideal_ys: list of ideal functions
        :returns: Ideal_Data_Point object
        '''
        min_deviation = float("inf")
        ideal = None
        for i, y in enumerate(ideal_ys):
            deviation = self.Finding_squares_deviation([train_y, y])
            if deviation < min_deviation:
                min_deviation = deviation
                ideal = Ideal_Data_Point(x, y, number=i, deviation=deviation)
        return ideal
    
    def get_ideal_for_test(self, test, ideal_functions):
        '''
        Chooses ideal function for test function
        Parameter test: Node_Test object
        Parameter ideal_functions: List of Ideal_Data_Point objects
        :returns: Ideal_Data_Point object
        '''
        choosen = None
        for ideal in ideal_functions:
            deviation = self.get_dev(ideal.y, test.y)
            if deviation <= math.sqrt(ideal.deviation):
                if choosen is None or choosen.deviation > ideal.deviation:
                    choosen = ideal
        return choosen

    
if __name__=="__main__":
 
    train = os.path.abspath('train.csv')
    test = os.path.abspath('test.csv')
    ideal = os.path.abspath('ideal.csv')
    database = Creating_Database(train, ideal)
    Assigninig = Matching_Functions(database, test)
    Assigninig.process_test_data()
   
