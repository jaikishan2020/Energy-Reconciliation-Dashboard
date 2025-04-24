# Objective
THe purpose of this dashboard to understand the Energy values aggregated for a time window for an Energy tree to understand reconciliated value between the Parent and its respctive children values from the real time data of Respective energy meter nodes ,received thro MQTT Broker .
Once the meter nodes are selected in drop down the processing starts and displays the processed data for the past 15 minutes from the moment the selection is made in the drop down.

# Energy Monitoring Dashboard

A real-time energy dashboard using Flask + Dash + MQTT. Visualizes parent-child meter hierarchy with reconciliation logic.

## Features
- Live MQTT data visualization
- Energy hierarchy using Dash Cytoscape
- Parent-child reconciliation report

## To Run Locally
```bash
pip install -r requirements.txt
python app.py
