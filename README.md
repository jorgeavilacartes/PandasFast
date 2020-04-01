# PandasFast
Faster computation of `apply` and `progress_apply` functions from pandas, using concurrent library. Parallel computation using Threads.

___
___
[ESP]
# **La clase Pandas Fast**

Una clase que utiliza la librería `concurrent` para realizar cómputos utilizando `Threads`, extendiendo algunas funcionalidades de pandas. Actualmente están implementados: 

- `apply`
- `progress_apply`
- `to_csv`
- `to_pickle`
- `to_excel`
- `head`
- `tail`

___ 
## `PandasFast(df, processes = None)`
- Debe ser inicializada con un data frame `df`, el cual debe ser del tipo `pandas.core.DataFrame`. 
- Está la opción de definir la cantidad de `Threads` a utilizar, mediante la variable `processes`. Se le da este nombre a la variable en lugar de llamar `threads` o `n_threads` porque se siguen los mismos nombres que utilizan las funciones, en este caso, la variable `processes` es un input para la clase `ThreadPoolExecutor` que nos ayuda a realizar la computación de manera concurrente. Por default está inicializada como `None`, esto asignará 2 `Threads`.
___
## **`apply` y `progress_apply`**
Recordemos que estas dos funciones hacen exactamente lo mismo, la única diferencia es que `progress_apply` utiliza internamente la librería `tqdm`, que nos permite tener una barra de progreso para tener un mayor control sobre el proceso.

**RECORDATORIO** Tanto `apply` como `progress_apply` nos permiten aplicar una función a cada componente de la (o las) columna(s) deseada(s). También se puede elegir hacerlo por filas, pero sólo están implementadas las funcionalidades por columnas.


En el caso de la clase `PandasFast`, estas funcionan de la misma forma, pues reciben: 

**Argumentos**
- `fun`: función que se quiere aplicar a una o más columnas, si tiene una variable de input, se asume que se aplica sobre una columna, si tiene dos, se aplica a dos columnas, y en el orden ingresado en la variable `columns`. Esta puede ser una función definida con `lambda` o definida afuera de la clase con `def`
- `columns`: un `str` con el nombre de una columna del  `df` o una lista de columnas. También puede ser una lista con el nombre de una sola columna, al igual que en pandas. **CUIDADO**, el la cantidad de columnas que deben ir en esta variable debe ser igual a la cantidad de inputs que recibe la función `fun`, y en el mismo orden en el que se solicita.

**Opcional**
- `new_columns`: un `str` o una lista con nombres de las nuevas columnas. Esto depende de la cantidad de salidas que entregue `fun`.
- `inplace`: si es `True`, la(s) nueva(s) columnas generadas serán incorporadas 
___

[ENG]
```
from PandasFast import PandasFast

import pandas as pd
from time import sleep

df = pd.DataFrame(
    [[1,5,2,2],
     [2,5,3,1],
     [3,3,2,1],
     [4,1,4,4]]
    , columns = ['A','B','C','D'])
df

# 1. Initialize PandasFast
pdfast = PandasFast(df)

# 2. Define the function you want to apply
def fun(x):
    sleep(1)
    return x + 1

# 3. Define the column you want to work on
columns = "B" # o también puede llamarse como lista ["B"]

# 4. Run apply function
pdfast.apply(fun, columns)
```

There is more! 

**What if I want to:**

- Work with two columns as inputs
```
def fun_cols(x,y):
    sleep(1)
    return x + y
    
columns = ["A","B"]

pdfast.apply(fun_cols,columns)
``` 

- Want to add specifics colnames to the outputs
```
columns = ["A","B"]
new_columns = "sum"
pdfast.apply(fun,columns, new_columns)
``` 

- Want to add the output as new columns of df? 
```
columns = ["A","B"]
new_columns = "sum"
pdfast.apply(fun,columns, new_columns, inplace = True)
``` 

- Want a progress bar? Use `progress_apply` instead of `apply`:
```
pdfast.progress_apply(fun, columns)
```
Got to check the TUTORIALS to see all the functionalities. 
