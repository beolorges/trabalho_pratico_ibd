import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
    SELECT DISTINCT cras, regional FROM POP_CADUNICO;
'''
cursor.execute(SQL)
recset = cursor.fetchall()
con.close()

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBH',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

print(f'Tamanho de inserção: {len(recset)}')
i = 0
for cras, regiao in recset:
    if(cras == 'ENDERECO NAO GEORREFERENCIADO'):
        continue

    SQL_STRING = f'''
        INSERT INTO cras(nome, id_regiao)
        SELECT '{cras}', r.id
        FROM regiao r
        WHERE unaccent(r.nome) ilike unaccent('{regiao}') 
        RETURNING id
    '''
    cursor.execute(SQL_STRING)
    res = cursor.fetchone()

    if res == None:
        print('Error')

        arquivo = open('cria_insert_cras/error.sql', 'w+')
        arquivo.write(SQL_STRING)
        arquivo.write('\n\n')


con.commit()