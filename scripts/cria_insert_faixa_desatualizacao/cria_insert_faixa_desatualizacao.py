import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
    SELECT DISTINCT faixa_desatualizacao FROM POP_RUA;
'''

cursor.execute(SQL)
recset = cursor.fetchall()
con.close()


arquivo = open('cria_insert_faixa_desatualizacao/cria_insert_faixa_desatualizacao.sql', 'w+', encoding="utf-8")

for faixa, in recset:
    arquivo.write(f"INSERT INTO faixa_desatualizacao(descricao) VALUES('{faixa}');\n")  

arquivo.close()
