import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
SELECT DISTINCT (TIPO_ACIDENTE), DESC_TIPO_ACIDENTE FROM boletim_acidentes;
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


arquivo = open('cria_insert_tipo_acidente/cria_insert_tipo_acidente.sql', 'w+', encoding="utf-8")

i = 0
for codigo, descricao in recset:
    SQL_STRING = f'''
    INSERT INTO 
        tipo_acidente(codigo, descricao)
    VALUES('{codigo}', '{descricao}');\n\n'''

    arquivo.write(SQL_STRING)  
    cursor.execute(SQL_STRING)

    i = i + 1
    print(f'Número de inserções:{i}')

con.commit()
con.close()
arquivo.close()
