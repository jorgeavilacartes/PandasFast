import pandas as pd
import concurrent.futures as CF
from tqdm import tqdm

# Class PandasFast
class PandasFast():
    '''Emulate functionalities of pandas with concurrent computation'''
    def __init__(self, df, processes = None):
        # Assertions Errors
        assert isinstance(df, pd.core.frame.DataFrame), f'df must be a pandas.core.frame.DataFrame. Yours is {type(df)}'
        
        # Initialization of parameters
        self.df = df
        self.processes = 2 if processes is None else processes
    
    
    # apply    
    def apply(self, fun, columns, new_columns = None, inplace = False):
        """Concurrent computation of apply function
        Arguments:
            fun {function} -- function to apply to the columns.
            columns {str or list} -- Columns or columns where to apply the function fun. If one column is required, this can be written as string or a list with one element.
        
        Keyword Arguments:
            new_columns {str or list} -- Name of the new columns that will be generated after applying the function fun (default: {None})
            inplace {bool} -- If True, the new columns will be concatenated to the original data frame in the class (default: {False})
        
        Returns:
            pandas.core.frame.DataFrame -- Either the new columns or the full data frame with the new columns included. 
        """        
        if isinstance(columns,str):
            columns = [columns]
            
        if isinstance(new_columns,str):
            new_columns = [new_columns]
            
        # Assertions Errors
        assert all([column for column in self.df.columns]), f' {[column for column in columns if column not in self.df.columns]} not in df.columns'
            
        # Auxiliary function that allows one input 
        fun_thread = lambda x: fun(*x)
        
        # Input
        input_thread = tuple( self.df[columns].to_records(index=False) )
        
        # Parallel computation
        with CF.ThreadPoolExecutor(self.processes) as executor:
            output = list(executor.map(fun_thread, input_thread))
        
        if inplace is True: 
            self.df[new_columns] = pd.DataFrame(output, columns = new_columns)
            return self.df
        else: 
            return pd.DataFrame(output, columns = new_columns)
    
    # progress_apply
    def progress_apply(self, fun, columns, new_columns = None, inplace = False):
        """Concurrent computation of progress_apply function
        
        Arguments:
            fun {function} -- function to apply to the columns.
            columns {str or list} -- Columns or columns where to apply the function fun. If one column is required, this can be writen as string or a list with one element.
        
        Keyword Arguments:
            new_columns {str or list} -- Name of the new columns that will be generated after applying the function fun (default: {None})
            inplace {bool} -- If True, the new columns will be concatenated to the original data frame in the class (default: {False})
        
        Returns:
                pandas.core.frame.DataFrame -- Either the new columns or the full data frame with the new columns included. 
        """        
        if isinstance(columns,str):
            columns = [columns]
            
        if isinstance(new_columns,str):
            new_columns = [new_columns]
            
        # Assertions Errors
        assert all([column for column in self.df.columns]), f' {[column for column in columns if column not in self.df.columns]} not in df.columns'
            
        # Auxiliary function that allows one input 
        fun_thread = lambda x: fun(*x)
        
        # Input
        input_thread = tuple( self.df[columns].to_records(index=False) )
        
        # Parallel computation
        with CF.ThreadPoolExecutor(self.processes) as executor:
            output = list(tqdm(executor.map(fun_thread, input_thread), total = self.df.shape[0]))
        
        if inplace is True: 
            self.df[new_columns] = pd.DataFrame(output, columns = new_columns)
            return self.df
        else: 
            return pd.DataFrame(output, columns = new_columns)
    
    # Save data to csv
    def to_csv(self, dir_save):
        """Save the result in a csv file
         
         Arguments:
             dir_save {str} -- path where to save the file.
         """        
         self.df.to_csv(dir_save)
    
    # Save data to excel
    def to_excel(self, dir_save):
         """Save the result in an excel file
         
         Arguments:
             dir_save {str} -- path where to save the file.
         """        
         self.df.to_excel(dir_save)
    
    # Save data to pickle
    def to_pickle(self, dir_save):
        """Save the result in a pickle file
         
         Arguments:
             dir_save {str} -- path where to save the file.
         """        
         self.df.to_pickle(dir_save)
            
    # Heder 
    def head(self,n=None):
        """Returns first n rows of the data frame. If n is None, 5 rows will be displayed bye default.
        
        Keyword Arguments:
            n {int} -- [description] (default: {None})

        """        
        assert num is None or isinstance(n, int), f'n must be an integer. Yours {n}'

        if num is None:
            return self.df.head()
        else: 
            return self.df.head(num)
    
    # Tail
    def tail(self,num=None):
        """Returns last n rows of the data frame. If n is None, 5 rows will be displayed bye default.
        
        Keyword Arguments:
            n {int} -- number of rows to consider. (default: {None})
        """        
        assert num is None or isinstance(n, int), f'n must be an integer. Yours {n}'

        if num is None:
            return self.df.tail()
        else: 
            return self.df.tail(num)
            