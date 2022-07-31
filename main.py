import pandas as pds

from utility.sqlite_utils import SQLLiteUtils

# establishing the local database connection
Conn_Conncetion_String = "sqlite:///data.db"

# CSV Path for ideal, train, test datasets
train_csv_path = "data_sets/train.csv"
ideal_csv_path = "data_sets/ideal.csv"
test_csv_path = "data_sets/test.csv"


def dataframe(filename):
    """
    @return: Read CSV File and Convert to DF
    """
    return pds.read_csv(filename)


def empty_dataframe():
    """
    @return: Empty DataFrame
    """
    return pds.DataFrame()


# Identifying the 4 ideal ideal_df from the 50 ideal_df
def ideal_function_finder(tr_df, idl_df):
    """
    In this function we take the training and ideal function as an arguments
    And then the algorithm executes to find the ideal_df
    And returns the ideal ideal_df and maximum deviation
    """
    # Changing the col indexes for compatibility
    print(idl_df)
    tr_df.columns = tr_df.columns.str.replace('y', 'tr_y')
    idl_df.columns = idl_df.columns.str.replace('y', 'idl_y')

    # The both datasets are merged (trained data and ideal data) by using pds based on 'x'.
    merged_dataframe = pds.merge(tr_df, idl_df, how='inner', right_on='x', left_on='x')

    # Creating empty dataframe for keeping final resulting values.
    generate_ideal_func = empty_dataframe()
    generate_max = empty_dataframe()

    # iterate the combined dataframe
    column_ = [_col_ for _col_ in merged_dataframe.columns if 'tr_' in _col_]
    # here empty df is used to extract the ideal_ideal_df, for temporary
    
    for col_, i in enumerate(column_):

        temp_df = empty_dataframe()
        cols = [co_ for co_ in merged_dataframe.columns if 'idl_' in co_]
        for k in cols:
            temp_df[f"{k}_ls"] = (merged_dataframe[i] - merged_dataframe[k]) ** 2

        _win_ = str(temp_df.sum().idxmin()).split("_")[1]

        generate_ideal_func[[_win_]] = merged_dataframe[["idl_" + _win_]]

        # Generating the max deviations and storing it n a data frame
        temp_df_max_value = temp_df[f"idl_{_win_}_ls"].max()
        generate_max[_win_] = [temp_df_max_value ** (0.50)]

    generate_ideal_func.insert( loc=0, column='x', value = merged_dataframe['x'])

    return {  'max' : generate_max  , 'ideal': generate_ideal_func   }

def mapping_function(_test_df, ideal_, max_d):
    """
    In mapping_function function, the test and ideal values are passed as the arguments
    And then the process begins for finding the mapping
    The condition is given below
    """
    # The both datasets are merged (test data and ideal data) by using pds.
    _test_df['ideal_func'] = None
    merged_dataframe = _test_df.merge(ideal_, how='left', on=['x'])
    r1 = merged_dataframe.iterrows()

    for index, row in r1:
        
        # Assigning the float minimum and none every time
        min_delta_y = float('inf')
        i_func = None
        r2 = max_d.T.iterrows()

        for _j, _row_ in r2:

            delta_y = abs(row['y'] - row[_j])

            # So to assign any targeted ideal_df to any test data point,
            # the delta value should be less than or equals when we perform max deviation by square(2) factor
            
            delta_bool = min_delta_y > delta_y
            delta_value = _row_[0] * (2 ** (0.50)) >= delta_y
            
            if delta_value and delta_bool:
                i_func = _j
                min_delta_y = delta_y
                

        _test_df.loc[index, 'ideal_func'] = i_func

        # Keeping Default value to None
        _test_df.at[index, 'delta_y'] = None
        _test_df.at[index, 'idl_y'] = None

        # Checking if min_delta_y is greater than inf. 
        if float('inf') > min_delta_y:
            # if so reassign to min_delta_y, thus you assign the minimum values
           _test_df.at[index, 'delta_y'] = min_delta_y

        # Checking for i_func is exit. if so reassign to i_func
        if i_func:
            _test_df.at[index, 'idl_y'] = merged_dataframe[i_func][index]

    return _test_df
       

if __name__ == "__main__":
    # Connecting the database and initiating the connection
    sqlite_utils_ = SQLLiteUtils(db_str_conn=Conn_Conncetion_String)

    # Creating table_1: Loading the train dataset and sending to store in the database:
    sqlite_utils_.put_data(df=dataframe(train_csv_path), table_name='train', db_str_conn=Conn_Conncetion_String)

    # Creating table_2: Loading the ideal dataset and sending to store in the database:
    sqlite_utils_.put_data(df=dataframe(ideal_csv_path), table_name='ideal', db_str_conn=Conn_Conncetion_String)

    # Creating table_3: Loading the train dataset and sending to store in the database:
    sqlite_utils_.put_data(df=dataframe(test_csv_path), table_name='test', db_str_conn=Conn_Conncetion_String)

    # identifying the ideal ideal_df and maximum deviations
    func_df = ideal_function_finder(dataframe(train_csv_path), dataframe(ideal_csv_path))
    
    # Mapping the ideal ideal_df to the data
    _mapping_ = mapping_function(dataframe(test_csv_path), func_df['ideal'], func_df['max'])

    # Final step : Final matching created along with mapping and deviations
    _mapping_ =   _mapping_[['x', 'y', 'delta_y', 'ideal_func']]
    print(f"Ideal Function: \n {func_df['ideal']}")
    print(f"Max Deviations: \n {func_df['max']}")
    print(f"Mapping: \n {_mapping_}")
    sqlite_utils_.put_data(df=_mapping_, table_name='test_map', db_str_conn=Conn_Conncetion_String)

