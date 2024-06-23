import fundamentus
import pandas as pd

def importar_funtamentus():

    funds_acoes = pd.DataFrame(fundamentus.get_resultado_raw())
    #funds_acoes['Papel'] = funds_acoes.index
    funds_acoes.insert(0,'Papel',funds_acoes.index)
    funds_acoes = funds_acoes.reset_index(drop=True)
    print(funds_acoes)
    
    return funds_acoes

def formula_magica_ROE(df):
    # Filtros no data frame de Ações e aplicação da estratégia usando ROE e P/L

    df['L/P'] = (1/df['P/L']) # mudança 
    
    filtros = df[(df['Cotação'] <= 100) &
               (df['Liq.2meses'] >= 800000) &
               (df['Dív.Brut/ Patrim.'] >= 0) &
               (df['Dív.Brut/ Patrim.'] <= 4) &
               (df['ROE'] > 0.09)]

    # Aplicação da fórmula nas ações

    Oev = filtros.sort_values(by='L/P', ascending=False)
    Oev = Oev.reset_index(drop=True)
    Oev['Ordem L/P'] = Oev.index

    Oroic = Oev.sort_values(by='ROE', ascending=False)
    Oroic = Oroic.reset_index(drop=True)
    Oroic['Ordem ROE'] = Oroic.index
    dados = Oroic

    dados['Score'] = dados['Ordem L/P'] + dados['Ordem ROE']
    dados = dados.sort_values(by='Score', ascending=True)

    # Remove os Tickers duplicados
    dados['PapelF'] = dados['Papel'].str[:4]
    Ac = dados.drop_duplicates(subset='PapelF')

    Ac.reset_index(inplace=True)
    Ac = Ac.drop('index', axis=1)

    print(Ac)
    
    return Ac

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    csv1 = importar_funtamentus()
    #csv1.to_csv("funds_acoes.csv")
    
    csv2 = formula_magica_ROE(importar_funtamentus())
    #csv2.to_csv("formula_magica_ROE.csv")