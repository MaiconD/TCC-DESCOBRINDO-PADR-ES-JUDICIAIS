import pandas as pd


def processar_classe():

    caminho_entrada = r"C:\Users\MD\Downloads\TCC\df_classes.csv"
    caminho_saida = r"C:\Users\MD\Downloads\TCC\df_classes_formatado.csv"

    # Carregar o arquivo de entrada
    df_classes = pd.read_csv(caminho_entrada, encoding='utf-8')

    # Ajuste: se cod_item == cod_item_pai para itens específicos
    itens_autorreferenciados = [385, 1198, 547, 11099, 2, 268, 11427, 11028, 5, 1310]  # Lista de cod_item_pai que apontam para 0
    df_classes.loc[df_classes['cod_item'].isin(itens_autorreferenciados), 'cod_item_pai'] = df_classes['cod_item']

    # Obter códigos dos pais
    cod_pais = df_classes['cod_item_pai'].unique()

    # Criar DataFrame com os nomes dos pais
    df_pai_nome = df_classes[df_classes['cod_item'].isin(cod_pais)]
    df_pai_nome = df_pai_nome[['cod_item', 'nome']].drop_duplicates().reset_index(drop=True)
    df_pai_nome_renamed = df_pai_nome.rename(columns={
        'cod_item': 'cod_item_pai',
        'nome': 'nome_pai'
    })

    # Merge no DataFrame principal com os nomes dos pais
    df_classes = df_classes.merge(df_pai_nome_renamed, on='cod_item_pai', how='left')

    df_classes = df_classes.rename(columns={
        'cod_item': 'dadosBasicos.classeProcessual'
    })



    # Salvar CSV formatado
    df_classes.to_csv(caminho_saida, index=False, encoding='utf-8')

    print(df_classes)

    # Retornar resultados úteis
    return df_classes