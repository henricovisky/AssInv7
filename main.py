import fundamentus
import pandas as pd

def importar_funtamentus():

    funds_acoes = pd.DataFrame(fundamentus.get_resultado_raw())

    papeis = list(funds_acoes.index)

    print(funds_acoes)
    
    return funds_acoes


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    importar_funtamentus()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
