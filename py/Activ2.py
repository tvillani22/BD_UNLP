from pyspark.sql import SQLContext, Row, SparkSession, functions as F

#########---------------------------FUNCIONES--------------------------#########
#########----------------------------INCISO 1--------------------------#########
def dni_madre(df, dni):
  '''Devuelve el dni de la madre.

  PARÁMETROS:
  df: Pyspark Dataframe
  dni: int
  -----------
  RETORNA:
  out: int o None
    dni de la madre si ese dato está en el dataset. None, en caso contrario.
  '''
  res = df.filter(df.dni == dni).collect()
  if len(res) == 0:
    print(f'El DNI {dni} no está registrado en el dataset.', end=' ')
    return
  return res[0]['dni_madre']


def son_primos(df, i1, i2):
  '''Dados dos dni determina, si es posible, si son primos.

  PARÁMETROS:
  df: Pyspark Dataframe
  i1: int
  i2: int
  -----------
  RETORNA:
  out: bool o None
    bool si es posible determinarlo con el dataset. None, en caso contrario.
  '''
  dni_m1, dni_m2 = dni_madre(df, i1), dni_madre(df, i2)
  if dni_m1 is None and dni_m2 is None:
    print(f'Con la información disponible, no es posible conocer la relación entre {i1} y {i2}.')
    return 

  if None in [dni_m1, dni_m2]:
    dni =  dni_m1 if dni_m1 is not None else dni_m2
    if dni_madre(df, dni) is None:
      print(f'Y como el otro individuo no tiene abuela registrada, no es '
            f'posible conocer la relación entre {i1} y {i2}.')
      return
    else: 
      print(f'Pero como el otro individuo tiene abuela registrada, los '
            f'individuos {i1} y {i2} no son primos ni hermanos.')
      return False
  
  if dni_m1 == dni_m2:
    print(f'Los individuos {i1} y {i2} no son primos, pero son hermanos.')
    return False
  
  dni_a1, dni_a2 = dni_madre(df, dni_m1), dni_madre(df, dni_m2)
  if dni_a1 is None and dni_a2 is None:
    print(f'Con la información disponible, no es posible conocer la relación entre {i1} y {i2}.')
    return 
  if dni_a1 == dni_a2:
    print(f'Los individuos {i1} y {i2} son primos.')
    return True
  print(f'Los individuos {i1} y {i2} no son primos ni hermanos.')
  return False


#########----------------------------INCISO 2--------------------------#########
def es_ancestro(df, dni1 , dni2):
  '''Determina si un indivuo es ancestro de otro.

  PARÁMETROS:
  df : Pyspark Dataframe
  dni1 : int
  dni2 : int
  -----------
  RETORNA:
  out: bool
    True si el individuo de dni1 es ancestro del de dni2. False en caso contrario.
  '''
  ancestros = []
  dni = dni2
  while True:
    dni_m = dni_madre(df, dni)
    if dni_m is None:
      break
    else:
      ancestros.append(dni_m)
      dni = dni_m
  if dni1 in ancestros:
    print(f'El individuo de dni {dni1} es ancestro del de dni {dni2}.')
    return True
  else:
    print(f'El individuo de dni {dni1} no es ancestro del de dni {dni2}.')
    return False


#########----------------------------INCISO 3--------------------------#########
def inciso3(df):
    print('Buscando abuela con más descendientes...')
    df_base = df.drop('nombre').persist()
    dfj = df.filter(df.dni_madre.isNull()).drop('dni_madre')
    col_name_aux = 'dni'
    i = 1
    while True:
        col_name = 'dni_desc' + str(i)
        dfj_aux = df_base.withColumnRenamed('dni', col_name)
        dfj_aux = dfj.join(dfj_aux, dfj[col_name_aux] == dfj_aux['dni_madre'], 'inner').select(dfj['*'], dfj_aux[col_name]) 
        col_name_aux = col_name
        if dfj_aux.count() == 0:
            #dfj.show(1)
            break
        else:
            dfj.unpersist()
            dfj = dfj_aux.persist()
            i += 1
            df_base.unpersist()

    df_ = dfj.groupBy(dfj.nombre).agg(F.count('*').alias('num_descendientes')) 
    max_n_desc = df_.agg(F.max(df_['num_descendientes'])).first()[0]
    nomb, num_desc = df_.filter(df_.num_descendientes == max_n_desc).first()
    print(f'La abuela con más descendientes es {nomb}, con {num_desc}.')


#########----------------------------MAIN--------------------------#########
def main():
    spark_session = SparkSession.builder\
                                .master("local")\
                                .appName('Activ2')\
                                .getOrCreate()
    spark_context = spark_session._sc
    sql_context = SQLContext(spark_context, spark_session)
    file_path = '../Data/Genealogia/Genealogia.txt'
    gen = spark_context.textFile(file_path)
    gen = gen.map(lambda t: t.split('\t'))\
            .map(lambda t: Row(nombre = t[0],
                            dni = int(t[1]),
                            dni_madre = None if t[2] == 'None' else int(t[2])))
    df = sql_context.createDataFrame(gen).persist()

    while True:
        inciso = input('Ingresar inciso (1,2 ó 3) o 0 para salir: ')
        if inciso == '1':
            try:
                i1 = int(input('Ingresar primer DNI: '))
                i2 = int(input('Ingresar segundo DNI: '))
                son_primos(df, i1, i2)
            except:
                print('DNI inválido.')
        elif inciso == '2':
            try:
                i1 = int(input('Ingresar primer DNI: '))
                i2 = int(input('Ingresar segundo DNI: '))
                es_ancestro(df, i1, i2)
            except:
                print('DNI inválido.')
        elif inciso == '3':
            inciso3(df)
        elif inciso == '0':
            break
        else:
            print('Inciso iválido.')   


#########--------------------------------------------------------------#########
if __name__ == "__main__":
    main()