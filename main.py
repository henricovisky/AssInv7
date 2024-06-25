import fundamentus
import pandas as pd
import os
import yfinance as yf
import pymysql

from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, DATETIME
from sqlalchemy.exc import IntegrityError

agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def importar_funtamentus():

    funds_acoes = pd.DataFrame(fundamentus.get_resultado_raw())
    #funds_acoes['Papel'] = funds_acoes.index
    funds_acoes.insert(0,'Papel',funds_acoes.index)
    funds_acoes.insert(0,'update',agora)
    funds_acoes = funds_acoes.reset_index(drop=True)
    print(funds_acoes)
    
    return funds_acoes

def formula_magica_ROE(df):
    # Filtros no data frame de Ações e aplicação da estratégia usando ROE e P/L

    filtros = df[(df['Cotação'] <= 100) &
               (df['Liq.2meses'] >= 800000) &
               (df['Dív.Brut/ Patrim.'] >= 0) &
               (df['Dív.Brut/ Patrim.'] <= 4) &
               (df['ROE'] >= 0.10)]

    # Aplicação da fórmula nas ações

    Olp = filtros.sort_values(by='P/L', ascending=True)
    Olp = Olp.reset_index(drop=True)
    Olp['Ordem P/L'] = Olp.index

    Oroe = Olp.sort_values(by='ROE', ascending=False)
    Oroe = Oroe.reset_index(drop=True)
    Oroe['Ordem ROE'] = Oroe.index
    dados = Oroe

    dados['Score'] = dados['Ordem P/L'] + dados['Ordem ROE']
    dados = dados.sort_values(by='Score', ascending=True)

    # Remove os Tickers duplicados
    dados['PapelF'] = dados['Papel'].str[:4]
    Ac = dados.drop_duplicates(subset='PapelF')

    Ac.reset_index(inplace=True)
    Ac = Ac.drop('index', axis=1)

    print(Ac)
    
    return Ac

def hist_dividendos_5anos(Ac):
    # Histórico de dividendos das ações
    tickers = Ac['Papel'] + ".SA"

    dividendos = []
    segmento = []
    setor = []
    nome_c = []

    for ticker in tickers:
        try:
            ticker_data = yf.Ticker(ticker)
            div = ticker_data.dividends
            setr = ticker_data.info.get('sector', 'N/A')
            seg = ticker_data.info.get('industry', 'N/A')
            nc = ticker_data.info.get('shortName', 'N/A')

            # Lista para os dividendos dos últimos 5 anos
            anos = ['2023', '2022', '2021', '2020', '2019']
            div6 = [div.get(x, 0).sum() for x in anos]
            y = sum(div6)

        except Exception as e:
            # Handle error and append default values
            y, setr, seg, nc = 0, 'N/A', 'N/A', 'N/A'

        dividendos.append(y)
        setor.append(setr)
        segmento.append(seg)
        nome_c.append(nc)

    Ac['Div. 5A'] = dividendos
    Ac['PJ'] = (Ac['Div. 5A'] / 5) / 0.065
    Ac['Setor'] = setor
    Ac['Seg'] = segmento
    Ac['Nome Curto'] = nome_c

    Ac = Ac[['update', 'Papel', 'Cotação', 'P/L','Div.Yield','Mrg. Líq.', 'ROIC', 'ROE', 'Ordem P/L', 'Ordem ROE', 'Score','Div. 5A','PJ', 'Setor', 'Seg', 'Nome Curto']]
    
    return Ac

def db_criar():

    db_user = os.getenv("DB_USER")
    db_senha = os.getenv("DB_SENHA")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    # string de conexão com o banco de dados, passando as variáveis do .env
    engine = create_engine(f'mysql+pymysql://{db_user}:{db_senha}@{db_host}/{db_name}')

    # Instanciando o objeto MetaData para criar a tabela no banco de dados
    metadata = MetaData()

    # Cria a tabela 'funds_acoes' com todas as colunas necessárias
    funds_acoes = Table('funds_acoes', metadata,
        Column('id', Integer, primary_key=True),
        Column('update', DATETIME),
        Column('Papel', String(10)),
        Column('Cotação', DECIMAL(10, 2)),
        Column('P/L', DECIMAL(10, 2)),
        Column('P/VP', DECIMAL(10, 2)),
        Column('PSR', DECIMAL(10, 2)),
        Column('Div.Yield', DECIMAL(10, 5)),
        Column('P/Ativo', DECIMAL(10, 2)),
        Column('P/Cap.Giro', DECIMAL(10, 2)),
        Column('P/EBIT', DECIMAL(10, 2)),
        Column('P/Ativ Circ.Liq', DECIMAL(10, 2)),
        Column('EV/EBIT', DECIMAL(10, 2)),
        Column('EV/EBITDA', DECIMAL(10, 2)),
        Column('Mrg Ebit', DECIMAL(10, 2)),
        Column('Mrg. Líq.', DECIMAL(10, 2)),
        Column('Liq. Corr.', DECIMAL(10, 2)),
        Column('ROIC', DECIMAL(10, 5)),
        Column('ROE', DECIMAL(10, 5)),
        Column('Liq.2meses', DECIMAL(16, 2)),
        Column('Patrim. Líq', DECIMAL(16, 2)),
        Column('Dív.Brut/ Patrim.', DECIMAL(10, 2)),
        Column('Cresc. Rec.5a', DECIMAL(10, 2))
    )

    formula_magica = Table('formula_magica', metadata,
        Column('id', Integer, primary_key=True),
        Column('update', DATETIME),  # You might want to specify the data type and default value
        Column('Papel', String(10)),
        Column('Cotação', DECIMAL(10, 2)),
        Column('P/L', DECIMAL(10, 2)),
        Column('Div.Yield', DECIMAL(10, 5)),
        Column('Mrg. Líq.', DECIMAL(10, 4)),  # Adjusted to accommodate the precision
        Column('ROIC', DECIMAL(10, 5)),
        Column('ROE', DECIMAL(10, 5)),
        Column('Ordem P/L', Integer),
        Column('Ordem ROE', Integer),
        Column('Score', Integer),
        Column('Div. 5A', DECIMAL(10, 5)),
        Column('PJ', DECIMAL(10, 4)),
        Column('Setor', String(50)),  # Adjusted to accommodate the length
        Column('Seg', String(50)),  # Adjusted to accommodate the length
        Column('Nome Curto', String(50))  # Adjusted to accommodate the length
    )

    metadata.create_all(engine)

def db_alimentar(df1,df2):
    
    df1 = df1.replace([float('inf'), float('-inf')], pd.NA)
    df2 = df2.replace([float('inf'), float('-inf')], pd.NA)

    db_user = os.getenv("DB_USER")
    db_senha = os.getenv("DB_SENHA")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")

    # string de conexão com o banco de dados, passando as variáveis do .env
    engine = create_engine(f'mysql+pymysql://{db_user}:{db_senha}@{db_host}/{db_name}')

    df1.to_sql('funds_acoes', con=engine, if_exists='append', index=False)  # Converte a linha em um DataFrame de uma única linha e insere no banco de dados

    df2.to_sql('formula_magica', con=engine, if_exists='append', index=False)  # Converte a linha em um DataFrame de uma única linha e insere no banco de dados





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    csv1 = importar_funtamentus()
    #csv1.to_csv("funds_acoes.csv")

    csv2 = hist_dividendos_5anos(formula_magica_ROE(csv1))
    #csv2.to_csv("formula_magica.csv")

    #db_criar()
    db_alimentar(csv1,csv2) 