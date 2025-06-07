# import pm4py
# from pm4py.objects.log.util import dataframe_utils
# from pm4py.objects.conversion.log import converter as log_converter
# from pm4py.algo.discovery.inductive import algorithm as inductive_miner
# from pm4py.visualization.process_tree import visualizer as pt_visualizer
#
# from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
# from pm4py.visualization.dfg import visualizer as dfg_visualization
#
# from pm4py.objects.bpmn.exporter import exporter as bpmn_exporter
# from pm4py.visualization.bpmn import visualizer as bpmn_visualizer

from core import movimento
from core import processo

# Caminho do arquivo CSV de saída
arquivo_csv = r'C:\Users\MD\Downloads\TCC\justica_estadual\processos-tjpr\processos-tjpr_8.csv'

# Converte os campos de data do dataframe para o formato timestamp
df_movimento = movimento.processar_movimento()

df_processo = processo.processar_processo()

df_processo = df_processo.merge(df_movimento, on='cod_item', how='left')

# Exporta para CSV
df_processo.to_csv(arquivo_csv, index=False, encoding='utf-8')

# df = pm4py.format_dataframe(df_processo, case_id= 'dadosBasicos.numero',
#                            activity_key='nome_pai',
#                            timestamp_key='dataHoraMovimento')
# print(df.head())
#
# print(df['dataHoraMovimento'][0])
#
# df = log_converter.apply(df)
#
# process_tree = inductive_miner.apply(df)
#
# gviz = pt_visualizer.apply(process_tree)
# pt_visualizer.save(gviz, "process_tree.png")
#
# pt_visualizer.view(gviz)
#
# dfg = dfg_discovery.apply(df)
# gviz = dfg_visualization.apply(dfg)
# dfg_visualization.save(gviz, "dfg.png")
#
# bpmn_model = pm4py.discover_bpmn_inductive(df)
#
# bpmn_model = pm4py.discover_bpmn_inductive(df)
# gviz = bpmn_visualizer.apply(bpmn_model)
# bpmn_visualizer.save(gviz, "modelo_bpmn.png")