import psycopg2

con = psycopg2.connect(
    host='localhost', 
    database='REGIOESBHTEMP',
    user='postgres', 
    password='postgres')

cursor = con.cursor()

SQL = '''
    SELECT 
        tempo_vive_na_rua, 
        contato_parente, 
        data_nascimento, 
        idade, 
        sexo, 
        bolsa_familia,
        grau_instrucao,
        cor_raca,
        faixa_renda,
        val_remuneracao,
        cras,
        faixa_desatualizacao
    FROM POP_RUA;
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

i = 0
for tempo_vive_na_rua, contato_parente, data_nascimento, idade, sexo, bolsa_familia, grau_instrucao, cor_raca, faixa_renda, val_remuneracao, cras, faixa_desatualizacao in recset:
    if  cras == 'TPSA Endereзo FORA de TPSA' or cras == "ENDERECO NAO GEORREFERENCIADO" or cras == "Endereзo FORA de Territуrio TPSA":
        continue
    
    SQL_STRING = f'''
        INSERT INTO 
            populacao_rua(
                id_tempo_na_rua,
                data_nascimento,
                idade,
                bolsa_familia,
                id_contato_parente,
                id_grau_escolaridade,
                id_cor_raca,
                id_renda_familiar_per_capta, 
                valor_remuneracao_ultimo_mes,
                id_cras,
                id_desatualizacao,
                id_sexo,
                timestamp)
        SELECT t.id, '{data_nascimento}', '{idade}', '{bolsa_familia == 'SIM' }', cp.id, f.id, c.id, r.id, '{val_remuneracao if val_remuneracao != None else 0}', cr.id, fd.id, s.id, now()
        FROM 
            tempo_nas_ruas t,
            faixa_grau_escolaridade f,
            cor_raca c,
            renda_per_capta r,
            cras cr,
            faixa_desatualizacao fd,
            contato_parente cp,
            sexo s
        WHERE 
            unaccent(t.descricao) ilike unaccent('{tempo_vive_na_rua}')
            AND unaccent(f.descricao) ilike unaccent('{grau_instrucao}') 
            AND unaccent(c.descricao) ilike unaccent('{cor_raca}') 
            AND unaccent(r.descricao) ilike unaccent('{faixa_renda}')
            AND unaccent(cr.nome) ilike unaccent('{cras}')
            AND unaccent(fd.descricao) ilike unaccent('{faixa_desatualizacao}')
            AND unaccent(s.descricao) ilike unaccent('{sexo}')
            AND unaccent(cp.descricao) ilike unaccent('{contato_parente}')
        RETURNING ID;'''

    cursor.execute(SQL_STRING)
    res = cursor.fetchall()

    if (res == None  or len(res) != 1):
        arquivo = open('cria_insert_pop_rua/error.sql', 'w+', encoding="utf-8")
        arquivo.write(SQL_STRING)

        raise Exception
    
    i = i + 1
    print(f'Número de inserções:{i}')

con.commit()
con.close()
