import pandas as pd


def processar_movimento():

    caminho_entrada = r"C:\Users\MD\Downloads\TCC\df_movimentos.csv"
    caminho_saida = r"C:\Users\MD\Downloads\TCC\df_movimentos_formatado.csv"

    # Carregar o arquivo de entrada
    df_movimentos = pd.read_csv(caminho_entrada, encoding='utf-8')

    # Ajuste: se cod_item == cod_item_pai para itens específicos
    itens_autorreferenciados = [1, 14]  # Lista de cod_item_pai que apontam para 0
    df_movimentos.loc[df_movimentos['cod_item'].isin(itens_autorreferenciados), 'cod_item_pai'] = df_movimentos['cod_item']

    # Obter códigos dos pais
    cod_pais = df_movimentos['cod_item_pai'].unique()

    # Criar DataFrame com os nomes dos pais
    df_pai_nome = df_movimentos[df_movimentos['cod_item'].isin(cod_pais)]
    df_pai_nome = df_pai_nome[['cod_item', 'nome']].drop_duplicates().reset_index(drop=True)
    df_pai_nome_renamed = df_pai_nome.rename(columns={
        'cod_item': 'cod_item_pai',
        'nome': 'nome_pai'
    })

    # Merge no DataFrame principal com os nomes dos pais
    df_movimentos = df_movimentos.merge(df_pai_nome_renamed, on='cod_item_pai', how='left')

    df_movimentos = df_movimentos.rename(columns={
        'cod_item':'codigoMovimento',
    })

    # Salvar CSV formatado
    df_movimentos.to_csv(caminho_saida, index=False, encoding='utf-8')

    print(df_movimentos)

    # Retornar resultados úteis
    return df_movimentos
