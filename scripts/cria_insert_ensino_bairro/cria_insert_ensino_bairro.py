import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
SELECT 
    NOMEUP,
    Matricula_na_Educacao_Infantil,
	Matricula_no_Ensino_Fundamental,
	Tamanho_das_turmas_no_Ensino_Fundamental,
	indice_de_Aproveitamento_no_Ensino_Fundamental,
	Matricula_no_Ensino_Medio,
	Tamanho_das_turmas_no_ensino_medio,
	indice_de_Aproveitamento_no_Ensino_Medio,
    IOL_3_Educacao,
    IQVU_3_Educacao
FROM 
    QUALIDADE_VIDA_URBANA
WHERE
    ano = '2016';
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


arquivo = open('cria_insert_ensino_bairro/cria_insert_ensino_bairro.sql', 'w+', encoding="utf-8")

i = 0
for bairro, matricula_ei, matricula_ef, tam_turma_ef, ind_aproveitamento_ef, matricula_em, tam_turma_em, ind_aproveitamento_em, iol, iqvu in recset:
    SQL_STRING = f'''
    INSERT INTO 
        ensino_bairro(
            id_bairro,
            matricula_educacao_infantil,
            matricula_ensino_fundamental,
            tamanho_turmas_ensino_fundamental,
            indice_aproveitamento_ensino_fundamental,
            matricula_ensino_medio,
            tamanho_turmas_ensino_medio,
            indice_aproveitamento_ensino_medio,
            iol,
            iqvu
        )
    SELECT b.id, 
        '{float(matricula_ei.replace(',', '.')) if matricula_ei != None else 0}', 
        '{float(matricula_ef.replace(',', '.')) if matricula_ef != None else 0}', 
        '{float(tam_turma_ef.replace(',', '.')) if tam_turma_ef != None else 0}', 
        '{float(ind_aproveitamento_ef.replace(',', '.')) if ind_aproveitamento_ef != None else 0}', 
        '{float(matricula_em.replace(',', '.')) if matricula_em != None else 0}', 
        '{float(tam_turma_em.replace(',', '.')) if tam_turma_em != None else 0}', 
        '{float(ind_aproveitamento_em.replace(',', '.')) if ind_aproveitamento_em != None else 0}', 
        '{float(iol.replace(',', '.')) if iol != None else 0}', 
        '{float(iqvu.replace(',', '.')) if iqvu != None else 0}'
    FROM 
        bairro b
    WHERE 
        unaccent(b.nome) ilike unaccent('%{bairro}%');\n\n'''

    arquivo.write(SQL_STRING)  
    cursor.execute(SQL_STRING)

    i = i + 1
    print(f'Número de inserções:{i}')

con.commit()
con.close()
arquivo.close()
