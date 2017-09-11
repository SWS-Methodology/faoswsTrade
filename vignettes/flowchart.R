library(DiagrammeR)

# This file will generate an HTML page with a flow chart

grViz("
digraph trade {
  # a 'graph' statement
  graph [overlap = false, fontsize = 12]

  # several 'node' statements
  node [style = filled, shape = box,
        fontname = Helvetica]

  es_raw [label = 'Raw Eurostat data', shape = '@@1']
  tl_raw [label = 'Raw UNSD data', shape = '@@1']
  add_cpc_codes [label = 'Add CPC codes']
  add_m49_codes [label = 'Add M49 codes']
  aggregate_partners [label = 'Aggregation over partners']
  aggregation_fcl [label = 'Trade flow aggregation by FCL']
  complete_trade_flow_1 [label = 'Complete trade flow', shape = '@@1', fillcolor = '@@2']
  complete_trade_flow_2 [label = 'Complete trade flow', shape = '@@1', fillcolor = '@@2']
  complete_trade_flow_sub [label = 'Complete trade flow', shape = '@@1', fillcolor = '@@2']
  complete_trade_flow_disseminate [label = 'Complete trade flow', shape = '@@1', fillcolor = '@@3']
  es_conversion [label = 'Conversion factors for fclunits (specific)']
  es_data [label = 'Eurostat data'] 
  es_data_assess [label = 'Data content assessment']
  es_mapping [label = 'Mapping:
    country names (geonom),
    CN8 > FCL,
    FCL units,
    EUR to USD']
  #es_notes [label = 'Application of notes']
  validate_tools [label = 'Analysts intervention', shape = '@@4']
  interactive [label = 'Data validation', shape = '@@4']
  flags [label = 'Add flags']
  mirror [label = 'Mirroring for
    non-reporting countries']
  #outliers [label = 'Outlier and missing quantity
  #  detection and imputation']
  outliers [label = 'Missing quantity imputation']
  tl_conversion [label = 'Conversion factors for
    fclunits (general and specific)']
  tl_data [label = 'UNSD data'] 
  tl_data_assess [label = 'Data content assessment']
  tl_mapping [label = 'Mapping:
    country names (M49),
    HS > FCL,
    FCL units']
  #tl_notes [label = 'Application of notes']
  tl_preaggregate [label = 'Aggregate individual shipments']
  tl_recode_flows [label = 'Recode re-imports as imports
    and re-exports as exports']
  tl_remove_europe [label = 'Remove European countries']
  total_trade [label = 'Total trade', shape = '@@1', fillcolor = '@@2']
  total_trade_sub [label = 'Total trade', shape = '@@1', fillcolor = '@@2']
  total_trade_disseminate [label = 'Total trade', shape = '@@1', fillcolor = '@@3']
  unified_flow_1 [label = 'Unified trade flow', shape = '@@1']
  unified_flow_2 [label = 'Unified trade flow', shape = '@@1']
  uv_calculation [label = 'Unit value calculation']

  module [label = 'Module']
  data [label = 'Data', shape = '@@1']
  validation [label = 'Data for
    validation', shape = '@@1', fillcolor = '@@2']
  dissemination [label = 'Data for
    dissemination', shape = '@@1', fillcolor = '@@3']

  es_raw ->
  es_data_assess ->
  es_data -> 
  es_mapping ->
  es_conversion ->
  #es_notes ->
  unified_flow_1

  tl_raw ->
  tl_data_assess ->
  tl_data -> 
  tl_preaggregate ->
  tl_remove_europe ->
  tl_recode_flows ->
  tl_mapping ->
  tl_conversion ->
  #tl_notes ->
  unified_flow_1

  unified_flow_2 ->
  uv_calculation ->
  outliers ->
  aggregation_fcl ->
  add_cpc_codes ->
  add_m49_codes ->
  mirror ->
  flags ->
  complete_trade_flow_1

  complete_trade_flow_2 ->
  aggregate_partners -> total_trade
  total_trade -> module [arrowsize = 0, penwidth = 0]

  subgraph cluster0 {
    label = 'Legend'
    fontsize = 25

    module ->
    data ->
    validate_tools ->
    validation ->
    dissemination [arrowsize = 0, penwidth = 0]
  }

  complete_trade_flow_sub ->
  interactive

  total_trade_sub ->
  interactive

  interactive ->
  complete_trade_flow_disseminate ->
  total_trade_disseminate

  {rank = same es_data tl_data}
  {rank = same es_mapping tl_mapping}
}

[1]: 'diamond'
[2]: 'orange'
[3]: 'lightgreen'
[4]: 'oval'
")

