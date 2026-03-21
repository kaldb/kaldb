#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Create OpenSearch Dashboards index pattern, visualizations, and dashboards
# for the NYC taxi trip dataset.
#
# Usage:
#   ./scripts/create-nyc-taxi-dashboards.sh
#
# Prerequisites:
#   - Astra stack running with NYC taxi data ingested
#   - OpenSearch Dashboards reachable at localhost:5601
# ---------------------------------------------------------------------------

DASHBOARDS_URL="${DASHBOARDS_URL:-http://localhost:5601}"
INDEX="${INDEX:-nyc_taxis}"

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

# ---- 1. Index pattern ----
log "Creating index pattern '$INDEX' ..."
post_saved_object "index-pattern" "$INDEX" "$(jq -nc --arg idx "$INDEX" '{
  attributes: {
    title: $idx,
    timeFieldName: "@timestamp"
  }
}')"

# ---- 2. Visualizations ----

# Helper: build a vis saved-object body
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

default_search_source() {
  jq -nc '{query:{query:"",language:"lucene"},filter:[],indexRefName:"kibanaSavedObjectMeta.searchSourceJSON.index"}'
}

dashboard_search_source() {
  jq -nc '{query:{query:"",language:"lucene"},filter:[]}'
}

dashboard_options_json() {
  jq -nc '{useMargins:true,hidePanelTitles:false}'
}

SEARCH_SOURCE="$(default_search_source)"
DASHBOARD_SEARCH_SOURCE="$(dashboard_search_source)"
DASHBOARD_OPTIONS_JSON="$(dashboard_options_json)"

# --- Trips over time (hourly date histogram) ---
log "  trips by hour ..."
VIS="$(jq -nc '{
  title: "Trips by hour",
  type: "line",
  params: {
    addLegend: true, addTooltip: true, legendPosition: "right",
    categoryAxes: [{id:"cat1",type:"category",position:"bottom",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Hour"}}],
    valueAxes: [{id:"val1",name:"Count",type:"value",position:"left",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Trip count"}}],
    seriesParams: [{show:true,type:"line",mode:"normal",data:{id:"1",label:"Count"},valueAxis:"val1",drawLinesBetweenPoints:true,showCircles:false}],
    grid:{categoryLines:false}
  },
  aggs: [
    {id:"1",enabled:true,type:"count",schema:"metric",params:{}},
    {id:"2",enabled:true,type:"date_histogram",schema:"segment",params:{field:"@timestamp",interval:"1h",min_doc_count:1}}
  ]
}')"
post_saved_object "visualization" "nyc-trips-over-time" "$(make_vis "Trips by hour" "$VIS" "$SEARCH_SOURCE")"

# --- Fare by payment type (bar chart) ---
log "  fare by payment type ..."
VIS="$(jq -nc '{
  title: "Avg fare by payment type",
  type: "histogram",
  params: {addLegend:true,addTooltip:true,legendPosition:"right",
    categoryAxes:[{id:"cat1",type:"category",position:"bottom",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Payment type"}}],
    valueAxes:[{id:"val1",name:"Avg fare",type:"value",position:"left",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Avg fare ($)"}}],
    seriesParams:[{show:true,type:"histogram",mode:"stacked",data:{id:"1",label:"Avg fare"},valueAxis:"val1",drawLinesBetweenPoints:true,showCircles:true}],
    grid:{categoryLines:false}},
  aggs: [
    {id:"1",enabled:true,type:"avg",schema:"metric",params:{field:"fare_amount"}},
    {id:"2",enabled:true,type:"terms",schema:"segment",params:{field:"payment_type",orderBy:"1",order:"desc",size:10}}
  ]
}')"
post_saved_object "visualization" "nyc-fare-by-payment" "$(make_vis "Avg fare by payment type" "$VIS" "$SEARCH_SOURCE")"

# --- Passenger count breakdown (pie) ---
log "  passenger count breakdown ..."
VIS="$(jq -nc '{
  title: "Trips by passenger count",
  type: "pie",
  params: {type:"pie",addTooltip:true,addLegend:true,legendPosition:"right",isDonut:false},
  aggs: [
    {id:"1",enabled:true,type:"count",schema:"metric",params:{}},
    {id:"2",enabled:true,type:"terms",schema:"segment",params:{field:"passenger_count",orderBy:"1",order:"desc",size:10,otherBucket:false,missingBucket:false}}
  ]
}')"
post_saved_object "visualization" "nyc-passenger-count" "$(make_vis "Trips by passenger count" "$VIS" "$SEARCH_SOURCE")"

# --- Total revenue by vendor (horizontal bar) ---
log "  revenue by vendor ..."
VIS="$(jq -nc '{
  title: "Total revenue by vendor",
  type: "horizontal_bar",
  params: {addLegend:true,addTooltip:true,legendPosition:"right",
    categoryAxes:[{id:"cat1",type:"category",position:"left",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Vendor"}}],
    valueAxes:[{id:"val1",name:"Revenue",type:"value",position:"bottom",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Total ($)"}}],
    seriesParams:[{show:true,type:"histogram",mode:"normal",data:{id:"1",label:"Revenue"},valueAxis:"val1"}],
    grid:{categoryLines:false}},
  aggs: [
    {id:"1",enabled:true,type:"sum",schema:"metric",params:{field:"total_amount"}},
    {id:"2",enabled:true,type:"terms",schema:"segment",params:{field:"vendor_id",orderBy:"1",order:"desc",size:10}}
  ]
}')"
post_saved_object "visualization" "nyc-revenue-by-vendor" "$(make_vis "Total revenue by vendor" "$VIS" "$SEARCH_SOURCE")"

# --- Avg tip by rate code (table) ---
log "  avg tip by rate code ..."
VIS="$(jq -nc '{
  title: "Avg tip by rate code",
  type: "table",
  params: {perPage:10,showPartialRows:false,showMetricsAtAllLevels:false},
  aggs: [
    {id:"1",enabled:true,type:"avg",schema:"metric",params:{field:"tip_amount"}},
    {id:"3",enabled:true,type:"avg",schema:"metric",params:{field:"trip_distance"}},
    {id:"4",enabled:true,type:"avg",schema:"metric",params:{field:"total_amount"}},
    {id:"2",enabled:true,type:"terms",schema:"bucket",params:{field:"rate_code_id",orderBy:"1",order:"desc",size:10}}
  ]
}')"
post_saved_object "visualization" "nyc-tip-by-rate-code" "$(make_vis "Avg tip by rate code" "$VIS" "$SEARCH_SOURCE")"

# --- Trip distance over time (line, avg per hour) ---
log "  avg trip distance by hour ..."
VIS="$(jq -nc '{
  title: "Avg trip distance by hour",
  type: "line",
  params: {addLegend:true,addTooltip:true,legendPosition:"right",
    categoryAxes:[{id:"cat1",type:"category",position:"bottom",show:true,style:{},scale:{type:"linear"},labels:{show:true,truncate:100},title:{text:"Hour"}}],
    valueAxes:[{id:"val1",name:"Distance",type:"value",position:"left",show:true,style:{},scale:{type:"linear",mode:"normal"},labels:{show:true},title:{text:"Avg distance (mi)"}}],
    seriesParams:[{show:true,type:"line",mode:"normal",data:{id:"1",label:"Avg distance"},valueAxis:"val1",drawLinesBetweenPoints:true,showCircles:false}],
    grid:{categoryLines:false}},
  aggs: [
    {id:"1",enabled:true,type:"avg",schema:"metric",params:{field:"trip_distance"}},
    {id:"2",enabled:true,type:"date_histogram",schema:"segment",params:{field:"@timestamp",interval:"1h",min_doc_count:1}}
  ]
}')"
post_saved_object "visualization" "nyc-distance-over-time" "$(make_vis "Avg trip distance by hour" "$VIS" "$SEARCH_SOURCE")"

# ---- 3. Saved search (raw log browser) ----
log "  raw trip log search ..."
post_saved_object "search" "nyc-raw-trips" "$(jq -nc --arg idx "$INDEX" --arg search_source "$SEARCH_SOURCE" '{
  attributes: {
    title: "NYC Taxi Trips (raw)",
    description: "Browse individual taxi trip records in the synthetic recent timeline",
    columns: ["@timestamp","pickup_datetime","dropoff_datetime","passenger_count","trip_distance","fare_amount","tip_amount","total_amount","payment_type","vendor_id"],
    sort: [["@timestamp","desc"]],
    kibanaSavedObjectMeta: {
      searchSourceJSON: $search_source
    }
  },
  references: [
    {name: "kibanaSavedObjectMeta.searchSourceJSON.index", type: "index-pattern", id: $idx}
  ]
}')"

# ---- 4. Dashboard ----
log "Creating dashboard ..."

PANELS_JSON='[
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":0,"w":48,"h":14,"i":"1"},"panelIndex":"1","embeddableConfig":{},"panelRefName":"panel_0"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":14,"w":24,"h":12,"i":"2"},"panelIndex":"2","embeddableConfig":{},"panelRefName":"panel_1"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":24,"y":14,"w":24,"h":12,"i":"3"},"panelIndex":"3","embeddableConfig":{},"panelRefName":"panel_2"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":26,"w":24,"h":12,"i":"4"},"panelIndex":"4","embeddableConfig":{},"panelRefName":"panel_3"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":24,"y":26,"w":24,"h":12,"i":"5"},"panelIndex":"5","embeddableConfig":{},"panelRefName":"panel_4"},
  {"version":"2.11.1","type":"visualization","gridData":{"x":0,"y":38,"w":48,"h":12,"i":"6"},"panelIndex":"6","embeddableConfig":{},"panelRefName":"panel_5"},
  {"version":"2.11.1","type":"search","gridData":{"x":0,"y":50,"w":48,"h":16,"i":"7"},"panelIndex":"7","embeddableConfig":{},"panelRefName":"panel_6"}
]'

post_saved_object "dashboard" "nyc-taxi-demo" "$(jq -nc \
  --arg panels "$PANELS_JSON" \
  --arg options_json "$DASHBOARD_OPTIONS_JSON" \
  --arg search_source "$DASHBOARD_SEARCH_SOURCE" \
  '{
    attributes: {
      title: "NYC Taxi Trips",
      description: "Demo dashboard for NYC taxi trip data remapped into the current day window with a short future buffer",
      hits: 0,
      optionsJSON: $options_json,
      panelsJSON: $panels,
      timeRestore: true,
      timeTo: "now+1h",
      timeFrom: "now/d",
      kibanaSavedObjectMeta: {
        searchSourceJSON: $search_source
      }
    },
    references: [
      {name:"panel_0",type:"visualization",id:"nyc-trips-over-time"},
      {name:"panel_1",type:"visualization",id:"nyc-fare-by-payment"},
      {name:"panel_2",type:"visualization",id:"nyc-passenger-count"},
      {name:"panel_3",type:"visualization",id:"nyc-revenue-by-vendor"},
      {name:"panel_4",type:"visualization",id:"nyc-tip-by-rate-code"},
      {name:"panel_5",type:"visualization",id:"nyc-distance-over-time"},
      {name:"panel_6",type:"search",id:"nyc-raw-trips"}
    ]
  }')"

cat <<EOF

Done! Dashboard links:
  OpenSearch Dashboards:  $DASHBOARDS_URL/app/dashboards#/view/nyc-taxi-demo
  Raw trip search:        $DASHBOARDS_URL/app/discover#/view/nyc-raw-trips

Panels:
  1. Trips by hour             (hourly count)
  2. Avg fare by payment type  (bar chart)
  3. Trips by passenger count  (pie chart)
  4. Total revenue by vendor   (horizontal bar)
  5. Avg tip by rate code      (table with distance & total)
  6. Avg trip distance / hour  (line chart)
  7. Raw trip records           (saved search)
EOF
