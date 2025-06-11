import json
import os
import pandas as pd
from pm4py.objects.log.util import dataframe_utils


def extrair_info_movimento(mov):
    """
    Extrai informações de um movimento processual:
    - código do movimento (nacional ou local)
    - tipo ('nacional' ou 'local')
    - data/hora do movimento
    """
    if not isinstance(mov, dict):
        return pd.Series([None, None, None])

    data_hora = mov.get('dataHora')

    if 'movimentoNacional' in mov and mov['movimentoNacional'] is not None:
        codigo = mov['movimentoNacional'].get('codigoNacional')
        tipo = 'nacional'
    elif 'movimentoLocal' in mov and mov['movimentoLocal'] is not None:
        codigo = mov['movimentoLocal'].get('codigoPaiNacional')
        tipo = 'local'
    else:
        codigo = None
        tipo = None

    return pd.Series([codigo, tipo, data_hora])


def extrair_assunto_principal(assuntos):
    """
    Extrai o código do assunto principal de um processo.
    Prioriza 'codigoNacional', mas usa 'codigoAssunto' local como fallback.
    """
    if not isinstance(assuntos, list):
        return None

    for assunto in assuntos:
        if assunto.get('principal'):
            codigo_nacional = assunto.get('codigoNacional')
            if codigo_nacional:
                return codigo_nacional

            assunto_local = assunto.get('assuntoLocal')
            if isinstance(assunto_local, dict):
                return assunto_local.get('codigoAssunto')

    return None


def carregar_dados_json(pasta):
    """
    Lê e carrega todos os arquivos JSON de uma pasta em uma lista de dicionários.
    """
    dados = []

    for arquivo in os.listdir(pasta):
        if not arquivo.endswith('.json'):
            continue

        caminho_arquivo = os.path.join(pasta, arquivo)

        try:
            with open(caminho_arquivo, 'r', encoding='utf-8') as    f:
                json_data = json.load(f)
                if isinstance(json_data, list):
                    dados.extend(json_data)
                else:
                    dados.append(json_data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[ERRO] Falha ao ler '{caminho_arquivo}': {e}")

    return dados


def filtro(dados):
    """
        Filtra processos com mais de 1 movimento e movimento codigoPaiNacional diferente de 99999999
    """
    df = pd.json_normalize(dados)
    df['movimento'] = df['movimento'].apply(lambda movs: movs if isinstance(movs, list) else [])

    movimento_invalido = [999, 2001, 2002, 99999999]

    def movimento_invalido_presente(movs):
        for mov in movs:
            if not isinstance(mov, dict):
                continue

            codigo = None

            if 'movimentoNacional' in mov and mov['movimentoNacional'] is not None:
                codigo = mov['movimentoNacional'].get('codigoNacional')

            elif 'movimentoLocal' in mov and mov['movimentoLocal'] is not None:
                codigo = mov['movimentoLocal'].get('codigoPaiNacional')

            if codigo in movimento_invalido:
                return True

        return False

    df_filtrado = df[
        (df['movimento'].apply(len) > 1) &
        (~df['movimento'].apply(movimento_invalido_presente))
    ]

    return df_filtrado


def converter_e_ordenar_por_tempo(df):
    """
      Converte colunas de timestamp para datetime e ordena o DataFrame pela coluna de dataHoraMovimento.
    """

    df_convertido = dataframe_utils.convert_timestamp_columns_in_df(df)

    df_ordenado = df_convertido.sort_values('dataHoraMovimento').reset_index(drop=True)

    return df_ordenado


def processar_processo(pasta):
    """
    Processa os dados de uma pasta com arquivos JSON de processos judiciais.
    Realiza:
    - leitura dos arquivos
    - filtragem de processos
    - extração de movimentos e assuntos principais
    - exportação para CSV
    Retorna um DataFrame pronto para mineração de processos.
    """
    dados = carregar_dados_json(pasta)
    total_processos = len(dados)
    print(f"Total de processos no JSON: {total_processos}")

    df_filtrado = filtro(dados)

    print(f"Total de processos após filtro: {len(df_filtrado)}")

    # Explodir a coluna de movimentos
    df_explodido = df_filtrado.explode('movimento').reset_index(drop=True)

    # Extrair informações dos movimentos
    df_explodido[['codigoMovimento', 'tipoMovimento', 'dataHoraMovimento']] = df_explodido['movimento'].apply(
        extrair_info_movimento
    )

    # Converter para data e ordenar por data do movimento
    df_explodido = converter_e_ordenar_por_tempo(df_explodido)

    # Extração do assunto principal
    df_explodido['assuntoPrincipal'] = df_explodido['dadosBasicos.assunto'].apply(extrair_assunto_principal)

    df_explodido.drop(columns=['movimento', 'millisInsercao', 'dadosBasicos.assunto'], inplace=True)

    return df_explodido
