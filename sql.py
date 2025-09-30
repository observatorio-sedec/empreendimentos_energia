from etl_empreendimento_grandes import df_empreendimentos_grandes, df_empreendimentos_pequenos
import psycopg2
from conexão import conexao

def executar_sql():
    
    cursor = conexao.cursor()
    cursor.execute('SET search_path TO energia, public')
    
    criando_empreendimentos_grandes = f'''
        CREATE TABLE IF NOT EXISTS energia.empreendimentos_grandes (
            DatGeracaoConjuntoDados       TEXT,
            NomEmpreendimento             TEXT,
            IdeNucleoCEG                  NUMERIC,
            CodCEG                        TEXT,
            SigUFPrincipal                TEXT,
            SigTipoGeracao                TEXT,
            DscFaseUsina                  TEXT,
            DscOrigemCombustivel          TEXT,
            DscFonteCombustivel           TEXT,
            DscTipoOutorga                TEXT,
            NomFonteCombustivel           TEXT,
            DatEntradaOperacao            TEXT,
            MdaPotenciaOutorgadaKw        TEXT,
            MdaPotenciaFiscalizadaKw      NUMERIC,
            MdaGarantiaFisicaKw           TEXT,
            IdcGeracaoQualificada         TEXT,
            NumCoordNEmpreendimento       TEXT,
            NumCoordEEmpreendimento       TEXT,
            DatInicioVigencia             TEXT,
            DatFimVigencia                TEXT,
            DscPropriRegimePariticipacao  TEXT,
            DscSubBacia                   TEXT,
            DscMunicipios                TEXT
        );
        '''
    cursor.execute(criando_empreendimentos_grandes)
    
    criando_tabela_municipal = f'''
        CREATE TABLE IF NOT EXISTS energia.empreendimentos_pequenos (
            DatGeracaoConjuntoDados         TEXT,
            NumCNPJDistribuidora            TEXT,
            SigAgente                       TEXT,
            NomAgente                       TEXT,
            DscClasseConsumo                TEXT,
            DscSubGrupoTarifario            TEXT,
            SigUF                           TEXT,
            CodRegiao                       NUMERIC,
            NomRegiao                       TEXT,
            CodMunicipioIbge               NUMERIC,
            NomMunicipio                    TEXT,
            CodCEP                          TEXT,
            SigTipoConsumidor               TEXT,
            NumCPFCNPJ                      TEXT,
            NomTitularEmpreendimento        TEXT,
            CodEmpreendimento               TEXT,
            DthAtualizaCadastralEmpreend    TEXT,
            SigModalidadeEmpreendimento     TEXT,
            DscModalidadeHabilitado         TEXT,
            QtdUCRecebeCredito              NUMERIC,
            SigTipoGeracao                  TEXT,
            DscFonteGeracao                 TEXT,
            DscPorte                        TEXT,
            NumCoordNEmpreendimento         TEXT,
            NumCoordEEmpreendimento         TEXT,
            MdaPotenciaInstaladaKW          TEXT,
            NomSubEstacao                   TEXT,
            NumCoordESub                    TEXT,
            NumCoordNSub                    TEXT
        );

        '''
    cursor.execute(criando_tabela_municipal)
    
    verificando_existencia_consumidores = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='empreendimentos_grandes';
    '''
    verificando_existencia_setor = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_name='empreendimentos_pequenos';
    '''
    
    cursor.execute(verificando_existencia_consumidores)
    resultado_consumidores = cursor.fetchone()
    

    cursor.execute(verificando_existencia_setor)
    resultado_setor = cursor.fetchone()
   

    if resultado_consumidores[0] == 1:
        dropando_empreendimentos_grandes = '''
        TRUNCATE empreendimentos_grandes;
        '''
        cursor.execute(dropando_empreendimentos_grandes)
    else:
        pass
        
    if resultado_setor[0] == 1:
        dropando_tabela_municipal = '''
        TRUNCATE empreendimentos_pequenos;
        '''
        cursor.execute(dropando_tabela_municipal)
    else:
        pass


    inserindo_dados_consumidores = '''
        INSERT INTO energia.empreendimentos_grandes (
            DatGeracaoConjuntoDados,
            NomEmpreendimento,
            IdeNucleoCEG,
            CodCEG,
            SigUFPrincipal,
            SigTipoGeracao,
            DscFaseUsina,
            DscOrigemCombustivel,
            DscFonteCombustivel,
            DscTipoOutorga,
            NomFonteCombustivel,
            DatEntradaOperacao,
            MdaPotenciaOutorgadaKw,
            MdaPotenciaFiscalizadaKw,
            MdaGarantiaFisicaKw,
            IdcGeracaoQualificada,
            NumCoordNEmpreendimento,
            NumCoordEEmpreendimento,
            DatInicioVigencia,
            DatFimVigencia,
            DscPropriRegimePariticipacao,
            DscSubBacia,
            DscMunicipios
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    '''

    try:
        dados_consumidores = [
            (
                i['DatGeracaoConjuntoDados'], 
                i['NomEmpreendimento'], 
                i['IdeNucleoCEG'], 
                i['CodCEG'], 
                i['SigUFPrincipal'], 
                i['SigTipoGeracao'], 
                i['DscFaseUsina'], 
                i['DscOrigemCombustivel'], 
                i['DscFonteCombustivel'], 
                i['DscTipoOutorga'], 
                i['NomFonteCombustivel'], 
                i['DatEntradaOperacao'], 
                i['MdaPotenciaOutorgadaKw'], 
                i['MdaPotenciaFiscalizadaKw'], 
                i['MdaGarantiaFisicaKw'], 
                i['IdcGeracaoQualificada'], 
                i['NumCoordNEmpreendimento'], 
                i['NumCoordEEmpreendimento'], 
                i['DatInicioVigencia'], 
                i['DatFimVigencia'], 
                i['DscPropriRegimePariticipacao'], 
                i['DscSubBacia'], 
                i['DscMunicipios']
            )
            for _, i in df_empreendimentos_grandes.iterrows()
        ]

        cursor.executemany(inserindo_dados_consumidores, dados_consumidores)
        conexao.commit()

    except psycopg2.Error as e:
        print(f"Erro ao inserir dados consumidores: {e}")

    inserindo_dados_industrial = '''
        INSERT INTO energia.empreendimentos_pequenos (
            DatGeracaoConjuntoDados,
            NumCNPJDistribuidora,
            SigAgente,
            NomAgente,
            DscClasseConsumo,
            DscSubGrupoTarifario,
            SigUF,
            CodRegiao,
            NomRegiao,
            CodMunicipioIbge,
            NomMunicipio,
            CodCEP,
            SigTipoConsumidor,
            NumCPFCNPJ,
            NomTitularEmpreendimento,
            CodEmpreendimento,
            DthAtualizaCadastralEmpreend,
            SigModalidadeEmpreendimento,
            DscModalidadeHabilitado,
            QtdUCRecebeCredito,
            SigTipoGeracao,
            DscFonteGeracao,
            DscPorte,
            NumCoordNEmpreendimento,
            NumCoordEEmpreendimento,
            MdaPotenciaInstaladaKW,
            NomSubEstacao,
            NumCoordESub,
            NumCoordNSub
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    '''

    try:
        dados_industrial = [
            (
                i['DatGeracaoConjuntoDados'], 
                i['NumCNPJDistribuidora'], 
                i['SigAgente'], 
                i['NomAgente'], 
                i['DscClasseConsumo'], 
                i['DscSubGrupoTarifario'], 
                i['SigUF'], 
                i['CodRegiao'], 
                i['NomRegiao'], 
                i['CodMunicipioIbge'], 
                i['NomMunicipio'], 
                i['CodCEP'], 
                i['SigTipoConsumidor'], 
                i['NumCPFCNPJ'], 
                i['NomTitularEmpreendimento'], 
                i['CodEmpreendimento'], 
                i['DthAtualizaCadastralEmpreend'], 
                i['SigModalidadeEmpreendimento'], 
                i['DscModalidadeHabilitado'], 
                i['QtdUCRecebeCredito'], 
                i['SigTipoGeracao'], 
                i['DscFonteGeracao'], 
                i['DscPorte'], 
                i['NumCoordNEmpreendimento'], 
                i['NumCoordEEmpreendimento'], 
                i['MdaPotenciaInstaladaKW'], 
                i['NomSubEstacao'], 
                i['NumCoordESub'], 
                i['NumCoordNSub']
            )
            for _, i in df_empreendimentos_pequenos.iterrows()
        ]

        cursor.executemany(inserindo_dados_industrial, dados_industrial)
        conexao.commit()

    except psycopg2.Error as e:
        print(f"Erro ao inserir dados industriais: {e}")