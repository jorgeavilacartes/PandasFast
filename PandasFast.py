import pandas as pd
import concurrent.futures as CF
from tqdm import tqdm
# Nueva clase
# Nueva clase
class PandasFast():
    '''Emulate functionalities of pandas with concurrent computation'''
    def __init__(self, df, processes = None):
        # Assertions Errors
        assert isinstance(df, pd.core.frame.DataFrame), f'df must be a pandas.core.frame.DataFrame. Yours is {type(df)}'
        
        # Initialization of parameters
        self.df = df
        self.processes = 2 if processes is None else processes
    
   # Nueva clase
# Nueva clase
class PandasFast():
    """Emulate functionalities of pandas with concurrent computation
    
    Returns:
        [type] -- [description]
    """    
    
    def __init__(self, df, processes = None):
        # Assertions Errors
        assert isinstance(df, pd.core.frame.DataFrame), f'df must be a pandas.core.frame.DataFrame. Yours is {type(df)}'
        
        # Initialization of parameters
        self.df = df
        self.processes = 2 if processes is None else processes
    
    
    # apply    
    def apply(self, fun, columns, new_columns = None, inplace = False):
        """Faster computation of apply function
        
        Arguments:
            fun {[type]} -- [description]
            columns {[type]} -- [description]
        
        Keyword Arguments:
            new_columns {[type]} -- [description] (default: {None})
            inplace {bool} -- [description] (default: {False})
        
        Returns:
            [type] -- [description]
        """        
    
        if isinstance(columns,str):
            columns = [columns]
            
        if isinstance(new_columns,str):
            new_columns = [new_columns]
            
        # Assertions Errors
        assert all([column for column in self.df.columns]), f' {[column for column in columns if column not in self.df.columns]} not in df.columns'
        #TODO: corroborar que new_columns sea una lista
        #TODO:  corroborar que el largo de new_columns sea igual al largo del output de la funcion f
        
            
        # Auxiliar function that allows one input 
        fun_thread = lambda x: fun(*x)
        
        # Input
        input_thread  = tuple( self.df.loc[idx,columns].tolist() for idx in self.df.index )
        
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
        '''Faster computation of progress_apply function'''
        # Assertions Errors
        assert all([column for column in self.df.columns]), f' {[column for column in columns if column not in self.df.columns]} not in df.columns'
        #TODO: corroborar que new_columns sea una lista
        #TODO:  corroborar que el largo de new_columns sea igual al largo del output de la funcion f
        
        if isinstance(columns,str):
            columns = [columns]
            
        if isinstance(new_columns,str):
            new_columns = [new_columns]
            
        # Auxiliar function that allows one input 
        fun_thread = lambda x: fun(*x)
        
        # Input
        input_thread  = tuple( tuple(self.df.loc[idx,columns].tolist()) for idx in self.df.index )
        
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
         self.df.to_csv(dir_save)
    
    # Save data to csv
    def to_excel(self, dir_save):
         self.df.to_excel(dir_save)
    
    # Save data to csv
    def to_pickle(self, dir_save):
         self.df.to_pickle(dir_save)
            
    # Visualizaciones
    def head(self,num=None):
        if num is None:
            return self.df.head()
        else: 
            return self.df.head(num)
    
    def tail(self,num=None):
        if num is None:
            return self.df.tail()
        else: 
            return self.df.tail(num)
            