import sys
sys.path.append('../')
from MRE import Job

#########------------------PARAMETROS GENERALES------------------------#########
data_path = '../Data/website/'
inputDir = data_path + 'input/'
tempDir1 = data_path + "temp/temp1"
tempDir2 = data_path + "temp/temp2"

#########---------------------------FUNCIONES--------------------------#########
#########----------------------------INCISO 1--------------------------#########
def fmap(key, value, context):
    id_user = key
    id_page = value.split()[0]
    time = value.split()[1]
    context.write((id_user, id_page), (1, time)) 

def fred(key, values, context):
    id_user = key[0]
    id_page = key[1]
    t_total = 0
    n_total = 0
    for v in values:
        n_acum_loc = v[0]
        t_acum_loc = v[1]
        n_total += int(n_acum_loc)
        t_total += int(t_acum_loc)
    context.write((id_user, id_page), (n_total, t_total))

def fmap12(key, value, context):
    id_user = key
    context.write(id_user, 1)

def fred12(key, values, context):
    id_user = key
    num_sitios = 0
    for v in values:
        num_sitios += v
    context.write(id_user, num_sitios)

def fmap13(key, value, context):
    id_user = key
    num_sitios = value
    context.write(99, (id_user, num_sitios))

def fred13(key, values, context):
    num_sitios_max = 0
    id_user_nmax = None
    for v in values:
        id_user = v[0]
        num_sitios = int(v[1])
        if num_sitios >  num_sitios_max:
            num_sitios_max = num_sitios
            id_user_nmax = id_user
    context.write(99, (id_user_nmax, num_sitios_max))

#########----------------------------INCISO 2--------------------------#########
def fmap22(key, value, context):
    id_user = key
    id_page = int(value.split()[0])
    n_total = int(value.split()[1])
    t_total = int(value.split()[2])
    if n_total > context['umbral']: # umbral no incluido
        context.write(id_user, (id_page, n_total, t_total))

def fred22(key, values, context):
    id_user = key
    t_max = 0
    n_tmax = None
    id_page_tmax = None
    for v in values:
        id_page = int(v[0])
        n_total = int(v[1])          
        t_total = int(v[2])
        if t_total > t_max:
            t_max = t_total
            id_page_tmax = id_page
            n_tmax = n_total
    context.write(id_user, (id_page_tmax, n_tmax, t_max))

#########----------------------------INCISO 3--------------------------#########
def fmap32(key, value, context):
    id_user = key
    id_page = value.split()[0]
    n_visitas = int(value.split()[1])
    n_usuarios = 1 
    context.write(id_page, (n_visitas, n_usuarios))

def fred32(key, values, context):
    id_page = key
    n_visitas_tot = 0
    n_usuarios_tot = 0
    for v in values:
        n_visitas = v[0] 
        n_usuarios = v[1]
        n_visitas_tot += n_visitas
        n_usuarios_tot += n_usuarios
    if n_usuarios_tot >= context['usuarios_minimos']:
        context.write(id_page, (n_visitas_tot, n_usuarios_tot))
        
def fmap33(key, value, context):
    id_page = key
    n_visitas_tot = int(value.split()[0])
    n_usuarios_tot = int(value.split()[1])
    context.write(99, (id_page, n_visitas_tot, n_usuarios_tot))

def fred33(key, values, context):
    n_visitas_max = 0
    id_page_vmax = None
    n_usuarios_vmax = 0
    maximos = []
    # output nulo si en combiner ningun input cumple condicion
    for v in values:
        id_page = v[0]
        n_visitas = v[1]
        n_usuarios = v[2]
        if n_visitas > n_visitas_max:
            n_visitas_max = n_visitas
            maximos = [[id_page, n_visitas, n_usuarios]]
        elif n_visitas == n_visitas_max:
            maximos.append([id_page, n_visitas, n_usuarios])
    for maximo in maximos:
        context.write(99, tuple(maximo))


#########--------------------------EJERCICIOS--------------------------#########
def inciso1():
    outputDir = data_path + 'output/output1/'
    job = Job(inputDir, tempDir1, fmap, fred)
    job.setCombiner(fred)
    job2 = Job(tempDir1, tempDir2, fmap12, fred12)
    job2.setCombiner(fred12)
    job3 = Job(tempDir2, outputDir, fmap13, fred13)
    job3.setCombiner(fred13)
    success = job.waitForCompletion()
    if success:
        print('Job 1 completado con éxito.')
        success = job2.waitForCompletion()
    if success:
        print('Job 2 completado con éxito.')
        success = job3.waitForCompletion()
    if success:
        print('Job 3 completado con éxito.')
        with open(outputDir + 'output.txt', 'rt') as fh:    
            linea = next(fh)
            id_user_nmax = linea.rstrip().split()[1]
            num_sitios_max = linea.rstrip().split()[2]
        print(f'\nRESULTADO \nEl usuario que más páginas distintas visitó '
              f'fue el usuario número {id_user_nmax}, con {num_sitios_max}.')  


def inciso2(v):
    outputDir = data_path + "output/output2/"
    V = v  # parámetro de visitas minimas
    parametros = {'umbral': V} # umbral no incluido
    job = Job(inputDir, tempDir1, fmap, fred)
    job.setCombiner(fred)
    job2 = Job(tempDir1, outputDir, fmap22, fred22)
    job2.setParams(parametros)
    job2.setCombiner(fred22)
    success = job.waitForCompletion()
    if success:
        print('Job 1 completado con éxito.')
        success = job2.waitForCompletion()
    if success:
        print('Job 2 completado con éxito.')
        n_lineas = sum(1 for line in open(outputDir + 'output.txt', 'rt'))
        if n_lineas == 0:
            print(f'No hay página que haya sido visitada más de {V} veces '
                   'por un mismo usuario.')
        elif n_lineas < 10:
            with open(outputDir + 'output.txt', 'rt') as fh:    
                for linea in fh:
                    id_user, id_page, n_visitas, time = linea.split()
                    print(f'\nRESULTADO \nUsuario: {id_user}; '
                          f'Página: {id_page}; Tiempo total: {time}; '
                          f'Número de visitas: {n_visitas}.' )
        else:
            print('Resultado extenso, acceder en el output file.') 


def inciso3(u):
    outputDir = data_path + "output/output3/"
    U = u  # parámetro de usuarios minimos
    parametros = {'usuarios_minimos': U}
    job = Job(inputDir, tempDir1, fmap, fred)
    job.setCombiner(fred)
    job2 = Job(tempDir1, tempDir2, fmap32, fred32)
    job2.setCombiner(fred32)
    job2.setParams(parametros)
    job3 = Job(tempDir2, outputDir, fmap33, fred33)
    job3.setCombiner(fred33)

    success = job.waitForCompletion()

    if success:
        print('Job 1 completado con éxito.')
        success = job2.waitForCompletion()

    if success:
        print('Job 2 completado con éxito.')
        success = job3.waitForCompletion()

    if success:
        print('Job 3 completado con éxito.')
        with open(outputDir + 'output.txt', 'rt') as fh:
            if fh is not None:
                num_lineas = len(fh.readlines())
                fh.seek(0)
                if num_lineas == 1:
                    linea = next(fh)
                    print(f'\nRESULTADO \nLa página más visitada, entre aquellas que fueron '
                          f'visitadas por al menos {U} usuarios, fue la {id_page}, que fue '
                          f'visitada un total de {n_visitas} veces, por {n_usuarios} distintos.')
                elif num_lineas < 5:
                    print(f'\nRESULTADO \nLas páginas más visitadas, entre aquellas que fueron '
                          f'visitadas por al menos {U} usuarios, son:\n')
                    for linea in fh:
                        id_page, n_visitas, n_usuarios = linea.rstrip().split()[1:]
                        print(f'id_pag: {id_page}; número de visitas: {n_visitas}; '
                              f'número de visitas: {n_visitas}; número de usuarios: {n_usuarios}.') 
                else:
                    print('Resultado extenso, acceder en el output file.')
            else:
                print(f'No hay página que haya sido visitada por al menos {U} usuarios.')


#########----------------------------MAIN--------------------------#########
def main():
    while True:
        inciso = input('Ingresar inciso (1,2 ó 3) o 0 para salir: ')
        if inciso == '1':
            inciso1()
        elif inciso == '2':
            try:
                param = int(input('Ingrese número de visitas mínimas: ')) - 1 
                # corrige x visitas mínimas a umbral no incluido
                inciso2(param)
            except:
                print('Parámetro inválido.')
        elif inciso == '3':
            try:
                param = int(input('Ingrese número de usuarios distintos: '))
                inciso3(param)
            except:
                print('Parámetro inválido.')
        elif inciso == '0':
            break
        else:
            print('Inciso iválido.')   


#########--------------------------------------------------------------#########
if __name__ == "__main__":
    main()