import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
SELECT 
	PARENTESCO_RF,
	DATA_NASCIMENTO,
	IDADE,
	SEXO,
	BOLSA_FAMILIA,
	POP_RUA,
	GRAU_INSTRUCAO,
	COR_RACA,
	FAIXA_RENDA_FAMILIAR_PER_CAPITA,
	VAL_REMUNERACAO_MES_PASSADO,
	CRAS,
	FAIXA_DESATUALICACAO_CADASTRAL
FROM POP_CADUNICO;
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

print(f'Total de pessoas cadunico: {len(recset)}')
total = 0
i = 0
for PARENTESCO_RF, DATA_NASCIMENTO, IDADE, SEXO, BOLSA_FAMILIA, POP_RUA, GRAU_INSTRUCAO, COR_RACA, FAIXA_RENDA_FAMILIAR_PER_CAPITA, VAL_REMUNERACAO_MES_PASSADO, CRAS, FAIXA_DESATUALICACAO_CADASTRAL in recset:
    if DATA_NASCIMENTO == None or IDADE == None or PARENTESCO_RF == None or CRAS == 'TPSA Endereзo FORA de TPSA' or CRAS == "ENDERECO NAO GEORREFERENCIADO" or CRAS == "Endereзo FORA de Territуrio TPSA":
        continue
    
    SQL_STRING = f'''
        INSERT INTO 
            populacao_cadunico(
                id_parentesco_responsavel,
                data_nascimento,
                idade,
                bolsa_familia,
                eh_morador_de_rua,
                id_grau_escolaridade,
                id_cor_raca,
                id_renda_familiar_per_capta, 
                valor_remuneracao_ultimo_mes,
                id_cras,
                id_desatualizacao,
                id_sexo,
                timestamp)
        SELECT r.id, '{DATA_NASCIMENTO}', '{IDADE}', '{BOLSA_FAMILIA == 'SIM' }', '{POP_RUA == 'SIM' }', f.id, c.id, r.id, '{VAL_REMUNERACAO_MES_PASSADO if VAL_REMUNERACAO_MES_PASSADO != None else 0}', cr.id, fd.id, s.id, now()
        FROM 
            parentesco p,
            faixa_grau_escolaridade f,
            cor_raca c,
            renda_per_capta r,
            cras cr,
            faixa_desatualizacao fd,
            sexo s
        WHERE 
            unaccent(f.descricao) ilike unaccent('{GRAU_INSTRUCAO}') 
            AND unaccent(p.nome) ilike unaccent('{PARENTESCO_RF}') 
            AND unaccent(c.descricao) ilike unaccent('{COR_RACA}') 
            AND unaccent(r.descricao) ilike unaccent('{FAIXA_RENDA_FAMILIAR_PER_CAPITA}')
            AND unaccent(cr.nome) ilike unaccent('{CRAS}')
            AND unaccent(fd.descricao) ilike unaccent('{FAIXA_DESATUALICACAO_CADASTRAL}')
            AND unaccent(s.descricao) ilike unaccent('{SEXO}')
        RETURNING ID
            '''

    cursor.execute(SQL_STRING)  
    res = cursor.fetchall()

    if (res == None  or len(res) != 1):
        arquivo = open('cria_insert_pop_cadunico/error.sql', 'w+', encoding="utf-8")
        arquivo.write(SQL_STRING)

        raise Exception

    i = i + 1

    print(f'Número de inserções: {i}')

con.commit()
con.close()
