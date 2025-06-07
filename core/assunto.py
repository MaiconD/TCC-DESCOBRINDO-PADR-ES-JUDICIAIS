import pandas as pd


def processar_assunto():

    caminho_entrada = r"C:\Users\MD\Downloads\TCC\df_assuntos.csv"
    caminho_saida = r"C:\Users\MD\Downloads\TCC\df_assuntos_formatado.csv"

    # Carregar o arquivo de entrada
    df_assuntos = pd.read_csv(caminho_entrada, encoding='utf-8')

    # Ajuste: se cod_item == cod_item_pai para itens específicos
    itens_autorreferenciados = [1, 14]  # Lista de cod_item_pai que apontam para 0
    df_assuntos.loc[df_assuntos['cod_item'].isin(itens_autorreferenciados), 'cod_item_pai'] = df_assuntos['cod_item']

    # Obter códigos dos pais
    cod_pais = df_assuntos['cod_item_pai'].unique()

    # Criar DataFrame com os nomes dos pais
    df_pai_nome = df_assuntos[df_assuntos['cod_item'].isin(cod_pais)]
    df_pai_nome = df_pai_nome[['cod_item', 'nome']].drop_duplicates().reset_index(drop=True)
    df_pai_nome_renamed = df_pai_nome.rename(columns={
        'cod_item': 'cod_item_pai',
        'nome': 'nome_pai'
    })

    # Merge no DataFrame principal com os nomes dos pais
    df_assuntos = df_assuntos.merge(df_pai_nome_renamed, on='cod_item_pai', how='left')

    # Salvar CSV formatado
    df_assuntos.to_csv(caminho_saida, index=False, encoding='utf-8')

    print(df_assuntos)

    # Retornar resultados úteis
    return df_assuntos