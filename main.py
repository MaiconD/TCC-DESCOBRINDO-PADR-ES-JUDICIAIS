import os

from core import processo
from core import movimento1
from core import classe


def main():
    # Caminho da pasta raiz (com as subpastas dos tribunais)
    arquivo_csv = r'C:\Users\MD\Downloads\TCC\justica_estadual'

    df_movimento = movimento1.processar_movimento()
    df_classe = classe.processar_classe()

    # Percorre todas as subpastas
    for subpasta in os.listdir(arquivo_csv):
        caminho_subpasta = os.path.join(arquivo_csv, subpasta)
        if not os.path.isdir(caminho_subpasta):
            continue

        print(f"Processando: {subpasta}")

        # Processa os dados do processo e do movimento
        df_processo = processo.processar_processo(caminho_subpasta)

        # Merge para pegar o movimento pai
        df_processo = df_processo.merge(df_movimento, on='codigoMovimento', how='left')

        # Merge para pegar a classe pai
        df_processo = df_processo.merge(df_classe, on='dadosBasicos.classeProcessual', how='left')

        print(df_processo)

        # Salva o CSV na própria subpasta
        nome_csv = f"{subpasta}.csv"
        caminho_csv_saida = os.path.join(arquivo_csv, nome_csv)

        df_processo.to_csv(caminho_csv_saida, index=False, encoding='utf-8-sig')
        print(f"CSV gerado: {caminho_csv_saida}")


if __name__ == "__main__":
    main()
