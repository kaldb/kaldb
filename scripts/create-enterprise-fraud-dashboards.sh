#!/usr/bin/env bash
set -euo pipefail

DASHBOARDS_URL="${DASHBOARDS_URL:-http://localhost:5601}"
INDEX="${INDEX:-enterprise_fraud_demo}"

post_saved_object() {
  local type="$1" id="$2" body="$3"
  curl -sS \
    -H 'osd-xsrf: true' \
    -H 'Content-Type: application/json' \
    -X POST \
    "$DASHBOARDS_URL/api/saved_objects/$type/$id?overwrite=true" \
    -d "$body" >/dev/null
}

log() { printf '%s\n' "$*"; }

make_search_source() {
  local query="$1"
  jq -nc --arg query "$query" '{
    query: {query: $query, language: "lucene"},
    filter: [],
    indexRefName: "kibanaSavedObjectMeta.searchSourceJSON.index"
  }'
}

dashboard_search_source() {
  jq -nc '{query:{query:"",language:"lucene"},filter:[]}'
}

dashboard_options_json() {
  jq -nc '{useMargins:true,hidePanelTitles:false}'
}

make_vis() {
  local title="$1" vis_state="$2" search_source="$3"
  jq -nc \
    --arg title "$title" \
    --arg vis_state "$vis_state" \
    --arg search_source "$search_source" \
    --arg idx "$INDEX" \
    '{
      attributes: {
        title: $title,
        visState: $vis_state,
        uiStateJSON: "{}",
        description: "",
        version: 1,
        kibanaSavedObjectMeta: {searchSourceJSON: $search_source}
      },
      references: [
        {name: "kibanaSavedObjectMeta.searchSourceJSON.index", type: "index-pattern", id: $idx}
      ]
    }'
}

make_search() {
  local id="$1" title="$2" query="$3" columns_json="$4"
  local search_source
  search_source="$(make_search_source "$query")"
  post_saved_object "search" "$id" "$(jq -nc \
    --arg idx "$INDEX" \
    --arg title "$title" \
    --arg search_source "$search_source" \
    --argjson columns "$columns_json" \
    --arg time_from "$DEMO_TIME_FROM" \
    --arg time_to "$DEMO_TIME_TO" '{
      attributes: {
        title: $title,
        description: "",
        columns: $columns,
        sort: [["@timestamp","desc"]],
        timeRestore: true,
        timeFrom: $time_from,
        timeTo: $time_to,
        kibanaSavedObjectMeta: {searchSourceJSON: $search_source}
      },
      references: [
        {name: "kibanaSavedObjectMeta.searchSourceJSON.index", type: "index-pattern", id: $idx}
      ]
    }')"
}

SEARCH_SOURCE="$(make_search_source "")"
DASHBOARD_SEARCH_SOURCE="$(dashboard_search_source)"
DASHBOARD_OPTIONS_JSON="$(dashboard_options_json)"
DEMO_TIME_FROM="${DEMO_TIME_FROM:-now-90d}"
DEMO_TIME_TO="${DEMO_TIME_TO:-now+15m}"
DISCOVER_TIME_TO="${DEMO_TIME_TO//+/%2B}"
DISCOVER_TIME_RANGE="_g=(time:(from:'$DEMO_TIME_FROM',to:'$DISCOVER_TIME_TO'))"

log "Creating index pattern '$INDEX' ..."
post_saved_object "index-pattern" "$INDEX" "$(jq -nc --arg idx "$INDEX" '{
  attributes: {
    title: $idx,
    timeFieldName: "@timestamp"
  }
}')"

log "Creating enterprise fraud visualizations ..."

VIS="$(jq -nc '{
  title: "Fraud activity over time",
  type: "line",
  params: {
    addLegend: true,
    addTooltip: true,
    legendPosition: "right",
    categoryAxes: [{id:"cat1",type:"category",position:"bottom",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Time"}}],
    valueAxes: [{id:"val1",name:"Events",type:"value",position:"left",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Event count"}}],
    seriesParams: [{show:true,type:"line",mode:"normal",data:{id:"1",label:"Events"},valueAxis:"val1",drawLinesBetweenPoints:true,showCircles:false}],
    grid:{categoryLines:false}
  },
  aggs: [
    {id:"1",enabled:true,type:"count",schema:"metric",params:{}},
    {id:"2",enabled:true,type:"date_histogram",schema:"segment",params:{field:"@timestamp",interval:"1d",min_doc_count:1}}
  ]
}')"
post_saved_object "visualization" "enterprise-fraud-events-over-time" "$(make_vis "Fraud activity over time" "$VIS" "$SEARCH_SOURCE")"

VIS="$(jq -nc '{
  title: "Events by business unit",
  type: "horizontal_bar",
  params: {
    addLegend:true,
    addTooltip:true,
    legendPosition:"right",
    categoryAxes:[{id:"cat1",type:"category",position:"left",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Business unit"}}],
    valueAxes:[{id:"val1",name:"Count",type:"value",position:"bottom",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Events"}}],
    seriesParams:[{show:true,type:"histogram",mode:"normal",data:{id:"1",label:"Events"},valueAxis:"val1"}],
    grid:{categoryLines:false}
  },
  aggs: [
    {id:"1",enabled:true,type:"count",schema:"metric",params:{}},
    {id:"2",enabled:true,type:"terms",schema:"segment",params:{field:"business_unit",orderBy:"1",order:"desc",size:10}}
  ]
}')"
post_saved_object "visualization" "enterprise-fraud-by-business-unit" "$(make_vis "Events by business unit" "$VIS" "$SEARCH_SOURCE")"

VIS="$(jq -nc '{
  title: "Visible windows",
  type: "pie",
  params: {type:"pie",addTooltip:true,addLegend:true,legendPosition:"right",isDonut:false},
  aggs: [
    {id:"1",enabled:true,type:"count",schema:"metric",params:{}},
    {id:"2",enabled:true,type:"terms",schema:"segment",params:{field:"event_window",orderBy:"1",order:"desc",size:10,otherBucket:false,missingBucket:false}}
  ]
}')"
post_saved_object "visualization" "enterprise-fraud-visible-windows" "$(make_vis "Visible windows" "$VIS" "$SEARCH_SOURCE")"

VIS="$(jq -nc '{
  title: "Schema variants by producer",
  type: "histogram",
  params: {
    addLegend:true,
    addTooltip:true,
    legendPosition:"right",
    categoryAxes:[{id:"cat1",type:"category",position:"bottom",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Schema variant"}}],
    valueAxes:[{id:"val1",name:"Events",type:"value",position:"left",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Events"}}],
    seriesParams:[{show:true,type:"histogram",mode:"stacked",data:{id:"1",label:"Events"},valueAxis:"val1",drawLinesBetweenPoints:true,showCircles:true}],
    grid:{categoryLines:false}
  },
  aggs: [
    {id:"1",enabled:true,type:"count",schema:"metric",params:{}},
    {id:"2",enabled:true,type:"terms",schema:"segment",params:{field:"schema_variant",orderBy:"1",order:"desc",size:10}}
  ]
}')"
post_saved_object "visualization" "enterprise-fraud-schema-variants" "$(make_vis "Schema variants by producer" "$VIS" "$SEARCH_SOURCE")"

VIS="$(jq -nc '{
  title: "Exposure by channel",
  type: "table",
  params: {perPage:10,showPartialRows:false,showMetricsAtAllLevels:false},
  aggs: [
    {id:"1",enabled:true,type:"sum",schema:"metric",params:{field:"transaction_amount"}},
    {id:"2",enabled:true,type:"avg",schema:"metric",params:{field:"risk_score"}},
    {id:"3",enabled:true,type:"terms",schema:"bucket",params:{field:"channel",orderBy:"1",order:"desc",size:10}}
  ]
}')"
post_saved_object "visualization" "enterprise-fraud-exposure-by-channel" "$(make_vis "Exposure by channel" "$VIS" "$SEARCH_SOURCE")"

log "Creating saved searches ..."
make_search \
  "enterprise-fraud-conflict-examples" \
  "Field conflict examples" \
  "risk_score_keyword:* OR customer_tier_integer:* OR mfa_required_keyword:*" \
  '["@timestamp","business_unit","team","schema_variant","risk_score","risk_score_keyword","customer_tier","customer_tier_integer","mfa_required","mfa_required_keyword","message"]'

make_search \
  "enterprise-fraud-historical-window" \
  "Historical attack window" \
  "event_window:historical" \
  '["@timestamp","business_unit","team","fraud_ring","account_id","risk_score","transaction_amount","message"]'

log "Creating dashboard ..."
PANELS_JSON='[
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":0,"w":48,"h":14,"i":"1"},"panelIndex":"1","embeddableConfig":{},"panelRefName":"panel_0"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":14,"w":24,"h":12,"i":"2"},"panelIndex":"2","embeddableConfig":{},"panelRefName":"panel_1"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":24,"y":14,"w":12,"h":12,"i":"3"},"panelIndex":"3","embeddableConfig":{},"panelRefName":"panel_2"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":36,"y":14,"w":12,"h":12,"i":"4"},"panelIndex":"4","embeddableConfig":{},"panelRefName":"panel_3"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":26,"w":24,"h":12,"i":"5"},"panelIndex":"5","embeddableConfig":{},"panelRefName":"panel_4"},
  {"version":"2.11.1","type":"search","gridData":{"x":24,"y":26,"w":24,"h":12,"i":"6"},"panelIndex":"6","embeddableConfig":{},"panelRefName":"panel_5"},
  {"version":"2.11.1","type":"search","gridData":{"x":0,"y":38,"w":48,"h":16,"i":"7"},"panelIndex":"7","embeddableConfig":{},"panelRefName":"panel_6"}
]'

post_saved_object "dashboard" "enterprise-fraud-investigation" "$(jq -nc \
  --arg panels "$PANELS_JSON" \
  --arg options_json "$DASHBOARD_OPTIONS_JSON" \
  --arg search_source "$DASHBOARD_SEARCH_SOURCE" \
  --arg time_from "$DEMO_TIME_FROM" \
  --arg time_to "$DEMO_TIME_TO" \
  '{
    attributes: {
      title: "Enterprise Fraud Investigation",
      description: "Demo dashboard for field conflicts, isolated component failures, and on-demand restore of historical fraud evidence",
      hits: 0,
      optionsJSON: $options_json,
      panelsJSON: $panels,
      timeRestore: true,
      timeTo: $time_to,
      timeFrom: $time_from,
      kibanaSavedObjectMeta: {
        searchSourceJSON: $search_source
      }
    },
    references: [
      {name:"panel_0",type:"visualization",id:"enterprise-fraud-events-over-time"},
      {name:"panel_1",type:"visualization",id:"enterprise-fraud-by-business-unit"},
      {name:"panel_2",type:"visualization",id:"enterprise-fraud-visible-windows"},
      {name:"panel_3",type:"visualization",id:"enterprise-fraud-schema-variants"},
      {name:"panel_4",type:"visualization",id:"enterprise-fraud-exposure-by-channel"},
      {name:"panel_5",type:"search",id:"enterprise-fraud-conflict-examples"},
      {name:"panel_6",type:"search",id:"enterprise-fraud-historical-window"}
    ]
  }')"

cat <<EOF

Done! Dashboard links:
  Dashboard:  $DASHBOARDS_URL/app/dashboards#/view/enterprise-fraud-investigation
  Conflicts:  $DASHBOARDS_URL/app/discover#/view/enterprise-fraud-conflict-examples?$DISCOVER_TIME_RANGE
  Historical: $DASHBOARDS_URL/app/discover#/view/enterprise-fraud-historical-window?$DISCOVER_TIME_RANGE
EOF
