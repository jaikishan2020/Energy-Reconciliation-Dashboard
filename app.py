
import dash_cytoscape as cyto
import dash
from dash import html, dcc, Input, Output, State
from flask import Flask, render_template, request, redirect
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import paho.mqtt.client as mqtt
import os
import threading
import json
import uuid

# Load static files
df_parent_child = pd.read_excel("Parent_Child_ID_Mapping.xlsx", usecols=["Parent Node ID", "Child Node ID"])
df_meters = pd.read_excel("mMeters_AMRDatalog_VICTUS.xlsx")

# Initialize app
server = Flask(__name__, template_folder='templates', static_folder='static')
app = dash.Dash(__name__, server=server, requests_pathname_prefix='/dash/', routes_pathname_prefix='/dash/')

# MQTT DataFrame
columns = ["Date_time", "MeterID", "Value", "Energy_Units"]
df = pd.DataFrame(columns=columns)
lock = threading.Lock()
# Aggregation time in minutes
aggregation_time = 15

def validate_mapping(df_meters, df_parent_child):
    meter_ids = set(df_meters["MeterID"])
    amr_ids = set(df_meters["AMR_MeterID"])
    parent_ids = set(df_parent_child["Parent Node ID"])
    child_ids = set(df_parent_child["Child Node ID"])
    all_tree_ids = parent_ids.union(child_ids)
    
    missing_in_meters = all_tree_ids - meter_ids
    if missing_in_meters:
        print(f"⚠️ These tree nodes are missing in df_meters['MeterID']: {missing_in_meters}")

    return not missing_in_meters

validate_mapping(df_meters, df_parent_child)



@server.route("/")
def index():
    df_copy = df_meters.copy()
    parent_ids = df_parent_child["Parent Node ID"].unique()
    parent_nodes = {int(row["MeterID"]): row["Name"] for _, row in df_copy.iterrows() if row["MeterID"] in parent_ids}
    return render_template("parent_index.html", parent_nodes=parent_nodes)

@server.route("/set_parent", methods=["POST"])
def set_parent():
    selected = request.form.get("parent_node")
    return redirect(f"/dash/?selected_parent={selected}")

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='selected-parent-store'),
    html.H2(id='selected-parent-display'),
    cyto.Cytoscape(
        id='cytoscape-graph',
        style={'width': '100%', 'height': '90vh'},
        layout={'name': 'preset'},
        zoom=1,
        minZoom=0.1,
        maxZoom=5,
        userZoomingEnabled=True,
        userPanningEnabled=True,
        autounselectify=True,
        responsive=True,
        elements=[],
        stylesheet=[
            
            {'selector': 'node',
             'style': {
                 'label': 'data(label)',
                 'text-wrap': 'wrap',
                 'text-valign': 'center',
                 'text-halign': 'center',
                 'color': 'white',
                 'shape': 'rectangle',
                 # 'background-color': '#0074D9',
                 'width': 'label',
                 'height': 'label',
                 'font-size': '12px'
             }},
            {'selector': 'edge',
             'style': {
                 'curve-style': 'bezier',
                 'target-arrow-shape': 'triangle',
                 'line-color': '#ccc',
                 'target-arrow-color': '#ccc'
             }}
        ]
    ),
    dcc.Interval(id='interval-update', interval=5000, n_intervals=0)
])


@app.callback(
    Output('selected-parent-store', 'data'),
    Input('url', 'search')
)
def update_selected_parent_from_url(search):
    from urllib.parse import parse_qs
    if not search:
        return None
    parsed = parse_qs(search[1:])
    return int(parsed.get("selected_parent", [None])[0]) if "selected_parent" in parsed else None

@app.callback(
    Output('cytoscape-graph', 'elements'),
    Output('cytoscape-graph', 'layout'),
    Output('selected-parent-display', 'children'),
    Input('interval-update', 'n_intervals'),
    State('selected-parent-store', 'data')
)
def update_graph(n, selected_parent):
    global df
    if df.empty:
        return [], {'name': 'preset'}, "Waiting for data..."

    latest_time = df["Date_time"].max()
    start_time = latest_time - timedelta(minutes=aggregation_time+1)
    df_filtered = df[(df["Date_time"] >= start_time) & (df["Date_time"] <= latest_time)]
    df_agg = df_filtered.groupby("MeterID", as_index=False).agg({"Energy_Units": "sum"})
    print(f"✅ Live data received for parent meter {selected_parent}")
    print(f" Data aggregated from {start_time} to {latest_time} for Parent Node {selected_parent} ")
    print(f"🔎 Filtered rows for aggregation: {len(df_filtered)}")
    print("📊 Aggregated Data:\n", df_agg[df_agg["MeterID"] == selected_parent])


    elements = build_elements(df_agg, selected_parent)
    layout = {
        'name': 'breadthfirst',
        'fit': False,
        'directed': True,
        'spacingFactor': 1.75,
        'animate': True,
        'roots': f'[id = "{selected_parent}"]' if selected_parent else f'[id = "{compute_roots()[0]}"]',
        'padding': 30,
        'nodeDimensionsIncludeLabels': True,
        'uid': str(uuid.uuid4())
    }

    parent_name = df_meters[df_meters["MeterID"] == selected_parent]["Name"].values[0] if selected_parent else "All Nodes"
    title_text = (
                f"Live Energy (units) Reconciliation of {parent_name} "
                f"from {start_time.strftime('%H:%M')} to {latest_time.strftime('%H:%M')} "
                f"on {start_time.strftime('%d %b %Y')}"
            ) if selected_parent else (
                f"Live Energy (units) Full Tree Overview "
                f"from {start_time.strftime('%H:%M')} to {latest_time.strftime('%H:%M')} "
                f"on {start_time.strftime('%d %b %Y')}"
            )
    if elements:
        print("🧪 Parent node element:", elements[0])
        print("🧪 Parent node element:", elements[-1])
    else:
        print("⚠️ No elements to render yet. Possibly waiting for incoming MQTT data.")

    return elements, layout, title_text


# MQTT functions (simplified)
def parse_xml_message(xml_message):
    try:
        root = ET.fromstring(xml_message)
        date_time = datetime.strptime(root.find("Date_time").text, "%d %b %Y %I:%M %p")
        meter_id = int(root.find("MeterID").text)
        value = float(root.find("Value").text)
        return date_time, meter_id, value
    except:
        return None

def update_dataframe(xml_message):
    global df
    parsed = parse_xml_message(xml_message)
    if parsed:
        dt, amr_meter_id, value = parsed
        try:
            # Map AMR_MeterID to local MeterID
            meter_match = df_meters[df_meters["AMR_MeterID"] == amr_meter_id]
            if meter_match.empty:
                print(f"⚠️ AMR_MeterID {amr_meter_id} not found in mapping.")
                return
            meter_id = int(meter_match["MeterID"].values[0])  # get corresponding MeterID
        except Exception as e:
            print(f"Error mapping AMR_MeterID: {e}")
            return

        prev = df[df["MeterID"] == meter_id].sort_values("Date_time").tail(1)
        energy = value - prev["Value"].values[0] if not prev.empty else 0
        row = pd.DataFrame([[dt, meter_id, value, energy]], columns=df.columns)
        row["Date_time"] = pd.to_datetime(row["Date_time"])
        with lock:
            df = pd.concat([df, row], ignore_index=True).sort_values(by=["MeterID", "Date_time"])
        print(df.head(5))
        print(df.tail(5))


def get_subtree_links(parent_id):
    visited = set()
    links = []

    def dfs(current):
        children = df_parent_child[df_parent_child["Parent Node ID"] == current]["Child Node ID"].tolist()
        for child in children:
            if (current, child) not in visited:
                visited.add((current, child))
                links.append((current, child))
                dfs(child)

    dfs(parent_id)
    return links


def build_elements(df_agg, selected_parent=None):
    elements = []
    node_ids_added = set()

    if not selected_parent:
        return []


    if selected_parent:
        subtree = get_subtree_links(selected_parent)
        filtered_links = pd.DataFrame(subtree, columns=["Parent Node ID", "Child Node ID"])
    else:
        filtered_links = df_parent_child

    for _, row in filtered_links.iterrows():
        parent_id = str(row["Parent Node ID"])
        child_id = str(row["Child Node ID"])
        elements.append({'data': {'source': parent_id, 'target': child_id}})

    node_ids = pd.unique(filtered_links[["Parent Node ID", "Child Node ID"]].values.ravel('K'))
    for node_id in node_ids:
        node_id_str = str(node_id)
        node_id_int = int(node_id)
        meter_row = df_meters[df_meters["MeterID"] == node_id_int]
        meter_name = meter_row["Name"].values[0] if not meter_row.empty else f"Meter {node_id_str}"
        actual = df_agg[df_agg["MeterID"] == int(node_id)]["Energy_Units"].sum()
        print(f"🔍 Node {node_id}: actual={actual:.2f}")
        if node_id_int in df_parent_child["Parent Node ID"].values:
            try:
                print(f"   ↪ Children: {child_ids} | sum={child_sum:.2f}")
            except Exception as e:
                print(f"[WARN] Could not compute children for node {node_id_int}: {e}")
                child_ids = []
                child_sum = 0


    
        label = f"{meter_name}" f"\nID:{node_id_str}" f"\nActual: {actual:.2f}"
        color = "blue"
        shape_style = {}
        height_style = {}

        if node_id_int in df_parent_child["Parent Node ID"].values:
            try:
                child_ids = df_parent_child[df_parent_child["Parent Node ID"] == node_id_int]["Child Node ID"].tolist()
                child_sum = df_agg[df_agg["MeterID"].isin(child_ids)]["Energy_Units"].sum()
                print(f"   ↪ Children: {child_ids} | sum={child_sum:.2f}")  # <- moved AFTER definition

            except Exception as e:
                print(f"[WARN] Could not compute children for node {node_id_int}: {e}")
                child_ids = []
                child_sum = 0

            reconciliation = ((actual - child_sum) / child_sum * 100) if child_sum else 0
            label += f"\nExpected: {child_sum:.2f}" f"\nRecon: {reconciliation:.2f}%"
            print(f"🟨 Color decision — Parent ID: {node_id_int}, Actual: {actual:.2f}, Expected: {child_sum:.2f}")
            actual = round(actual, 2)
            child_sum = round(child_sum, 2)
            if actual > child_sum:
                color = "yellow"
            elif actual < child_sum:
                color = "red"
            else    :
                color = "blue"

            print(f"🎨 Final color for parent node {node_id_int}: {color}")


            shape_style = {
                'shape': 'rectangle',
                'height': '120px',
                'text-wrap': 'wrap',
                'white-space': 'pre-wrap',
                'text-valign': 'center',
                'text-halign': 'center'
            }

        style = {
            'background-color': color,
            'color': 'white',
            'font-size': '14px',
            'padding': '10px',
            'text-max-width': '300px',
            'width': 'label',
        }
        style.update(shape_style)

        elements.append({
            'data': {'id': node_id_str, 'label': label},
            'style': style
        })

    return elements

def compute_roots():
    all_parents = set(df_parent_child["Parent Node ID"])
    all_children = set(df_parent_child["Child Node ID"])
    root_ids = list(all_parents - all_children)
    return root_ids


# MQTT callbacks
def on_message(client, userdata, msg):
    try:
        message = msg.payload.decode("utf-8")
        print(f"Received MQTT message: {message[:50]}...")
        update_dataframe(message)
    except Exception as e:
        print(f"Error processing MQTT message: {e}")


client = mqtt.Client()
client.on_message = on_message
client.connect("ITAMR.readmeter.in", 1883, 60)
client.subscribe("BroadcastTopic")
threading.Thread(target=client.loop_forever, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=True)
