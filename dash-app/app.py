import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from datetime import datetime
import json
import redis
from threading import Thread
from collections import deque
import time
from redis_client import get_all_taxi_ids, get_latest_location, get_route

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Redis Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_DB = 0

# Data storage
taxi_data = {}
incident_log = deque(maxlen=20)
geo_json = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "Dongcheng"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [116.39, 39.91], [116.42, 39.91], [116.42, 39.93], [116.39, 39.93], [116.39, 39.91]
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"name": "Xicheng"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [116.35, 39.91], [116.39, 39.91], [116.39, 39.93], [116.35, 39.93], [116.35, 39.91]
                ]]
            }
        }
    ]
}


# Blacklist for taxi IDs or incident types
BLACKLISTED_TAXIS = set()  
BLACKLISTED_INCIDENT_TYPES = set() 

def is_blacklisted_incident(incident):
    if incident['taxi_id'] in BLACKLISTED_TAXIS:
        return True
    if incident['type'] in BLACKLISTED_INCIDENT_TYPES:
        return True
    # Blacklist speed violations above 200 km/h
    if incident['type'] == 'Speed Violation' and incident['speed'] > 200:
        return True
    return False

# Redis data fetcher
def fetch_redis_data():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    while True:
        try:
            taxi_ids = list(set(get_all_taxi_ids() + [key.decode().split(":")[1] for key in r.keys("location:*")]))
            for taxi_id in taxi_ids:
                location_data = r.hgetall(f"location:{taxi_id}")
                latest = get_latest_location(taxi_id)
                if location_data:
                    taxi_data[taxi_id] = {
                        'lat': float(location_data.get(b'lat', 0)),
                        'lng': float(location_data.get(b'lon', 0)),
                        'speed': float(r.hget("metrics:speed", taxi_id) or 0),
                        'timestamp': location_data.get(b'time', b'').decode()
                    }
                elif latest:
                    taxi_data[taxi_id] = {
                        'lat': latest["latitude"],
                        'lng': latest["longitude"],
                        'speed': 0,  # Default speed if not available
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    }
                
                # Check for incidents (speed > 60 km/h)
                speed = taxi_data[taxi_id]['speed']
                if speed > 60:
                    incident = {
                        'taxi_id': taxi_id,
                        'type': 'Speed Violation',
                        'speed': speed,
                        'lat': taxi_data[taxi_id]['lat'],
                        'lng': taxi_data[taxi_id]['lng'],
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    }
                    # Blacklist check
                    if not is_blacklisted_incident(incident):
                        # Check if this incident is already logged
                        if not any(i['taxi_id'] == taxi_id and
                                   i['speed'] == speed and
                                   i['timestamp'] == incident['timestamp']
                                   for i in incident_log):
                            incident_log.appendleft(incident)
            time.sleep(1)  # Poll every second

        except Exception as e:
            print(f"Redis error: {e}")
            time.sleep(5)  # Wait before retrying if error occurs

# Start Redis fetcher in a separate thread
redis_thread = Thread(target=fetch_redis_data, daemon=True)
redis_thread.start()

# Dashboard layout
app.layout = dbc.Container(fluid=True, children=[
    # Header
    dbc.Navbar(
        dbc.Container([
            html.Div([
                html.I(className="fas fa-taxi me-2"),
                html.Span("Beijing Taxi Monitoring Dashboard", className="navbar-brand mb-0 h1")
            ], className="d-flex align-items-center"),

            html.Div([
                dbc.Badge([
                    html.Span(className="pulse-dot me-2"),
                    "Real-time"
                ], color="primary", className="me-3"),

                html.Div(id="current-time", className="text-white")
            ], className="d-flex align-items-center")
        ]),
        color="primary",
        dark=True
    ),

    # Main content
    dbc.Row([
        # Left column
        dbc.Col(md=8, children=[
            # Map
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Taxi Activity Map", className="card-title"),
                    html.Div([
                        dbc.Button(html.I(className="fas fa-search-plus"), color="light", className="me-2", id="zoom-in"),
                        dbc.Button(html.I(className="fas fa-search-minus"), color="light", className="me-2", id="zoom-out"),
                        dbc.Button(html.I(className="fas fa-globe-asia"), color="light", id="reset-view"),
                        dcc.Dropdown(
                            id="taxi-id",
                            options=[{"label": tid, "value": tid} for tid in get_all_taxi_ids()],
                            placeholder="Select a Taxi to Track",
                            style={"width": "300px", "margin-left": "10px"},
                            clearable=True
                        )
                    ], className="float-end")
                ], className="d-flex justify-content-between align-items-center"),

                dcc.Graph(
                    id='taxi-map',
                    config={'displayModeBar': False},
                    style={'height': '500px'}
                )
            ], className="mb-4"),

            # Stats cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                html.Div([
                                    html.P("Active Taxis", className="text-muted small mb-1"),
                                    html.H3(id="active-taxis", children="0", className="mb-0")
                                ]),
                                html.Div(
                                    html.I(className="fas fa-taxi fa-2x text-primary"),
                                    className="bg-primary bg-opacity-10 p-3 rounded-circle"
                                )
                            ], className="d-flex justify-content-between align-items-center")
                        ])
                    ])
                ], md=4),

                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                html.Div([
                                    html.P("Violations Today", className="text-muted small mb-1"),
                                    html.H3(id="violations-today", children="0", className="mb-0")
                                ]),
                                html.Div(
                                    html.I(className="fas fa-exclamation-triangle fa-2x text-danger"),
                                    className="bg-danger bg-opacity-10 p-3 rounded-circle"
                                )
                            ], className="d-flex justify-content-between align-items-center")
                        ])
                    ])
                ], md=4),

                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Div([
                                html.Div([
                                    html.P("Avg. Speed", className="text-muted small mb-1"),
                                    html.H3(id="avg-speed", children="0 km/h", className="mb-0")
                                ]),
                                html.Div(
                                    html.I(className="fas fa-tachometer-alt fa-2x text-success"),
                                    className="bg-success bg-opacity-10 p-3 rounded-circle"
                                )
                            ], className="d-flex justify-content-between align-items-center")
                        ])
                    ])
                ], md=4)
            ], className="mb-4")
        ]),

        # Right column
        dbc.Col(md=4, children=[
            # Incidents
            dbc.Card([
                dbc.CardHeader([
                    html.I(className="fas fa-exclamation-circle text-danger me-2"),
                    html.H4("Recent Incidents", className="d-inline-block mb-0")
                ]),

                dbc.CardBody([
                    html.Div(id="incidents-container", className="overflow-auto", style={"maxHeight": "400px"})
                ])
            ], className="mb-4"),

            # Speed chart
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Speed Distribution", className="card-title")
                ]),

                dbc.CardBody([
                    dcc.Graph(id="speed-chart", config={'displayModeBar': False}, style={'height': '200px'})
                ])
            ])
        ])
    ], className="mt-4"),

    # Footer
    dbc.Navbar(
        dbc.Container(
            html.P("Beijing Taxi Monitoring System ©️ 2023 | Real-time Stream Processing Dashboard",
                   className="text-center w-100 mb-0"),
        ),
        color="dark",
        dark=True,
        className="mt-4 py-3"
    ),

    # Interval for updates
    dcc.Interval(id='update-interval', interval=2000, n_intervals=0),
])

# CSS styles
app.css.append_css({
    'external_url': 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
})

app.css.append_css({
    'external_url': 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/leaflet.css'
})

app.css.append_css({
    'external_url': 'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap'
})

# Custom CSS
app.css.append_css({
    'external_url': '''
        .pulse-dot {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background-color: #0f0;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }
        
        .incident-card {
            transition: all 0.3s ease;
            padding: 10px;
            border-bottom: 1px solid #eee;
        }
        
        .incident-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            background-color: #f8f9fa;
        }
        
        .map-marker {
            background-color: #0d6efd;
            color: white;
            border-radius: 50%;
            width: 30px;
            height: 30px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        }
    '''
})

# Callbacks
@app.callback(
    Output('current-time', 'children'),
    Input('update-interval', 'n_intervals')
)
def update_time(n):
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@app.callback(
    [Output('taxi-map', 'figure'),
     Output('active-taxis', 'children'),
     Output('violations-today', 'children'),
     Output('avg-speed', 'children'),
     Output('speed-chart', 'figure'),
     Output('incidents-container', 'children')],
    [Input('update-interval', 'n_intervals'),
     Input('zoom-in', 'n_clicks'),
     Input('zoom-out', 'n_clicks'),
     Input('reset-view', 'n_clicks'),
     Input('taxi-id', 'value')],
    [State('taxi-map', 'relayoutData')]
)
def update_dashboard(n, zoom_in, zoom_out, reset_view, selected_taxi_id, relayout_data):
    ctx = dash.callback_context
    zoom_level = 12
    
    # Handle map zoom buttons
    if ctx.triggered:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if trigger_id == 'zoom-in':
            zoom_level = (relayout_data.get('mapbox.zoom', 12) + 1) if relayout_data else 13
        elif trigger_id == 'zoom-out':
            zoom_level = (relayout_data.get('mapbox.zoom', 12) - 1) if relayout_data else 11
        elif trigger_id == 'reset-view':
            zoom_level = 12
    
    # Create map figure
    map_fig = go.Figure()
    
    # Add districts
    for feature in geo_json['features']:
        lons = [lon for lon, lat in feature['geometry']['coordinates'][0]]
        lats = [lat for lon, lat in feature['geometry']['coordinates'][0]]
        
        map_fig.add_trace(go.Scattermap(
            lon=lons + [lons[0]],  # Close the polygon
            lat=lats + [lats[0]],
            mode='lines',
            line=dict(width=2, color='#3182ce'),
            fill='toself',
            fillcolor='rgba(49, 130, 206, 0.1)',
            name=feature['properties']['name'],
            hoverinfo='text',
            text=feature['properties']['name']
        ))
    
    # Add taxi markers if we have data
    if taxi_data:
        taxi_df = pd.DataFrame.from_dict(taxi_data, orient='index').reset_index()
        taxi_df.columns = ['taxi_id', 'lat', 'lng', 'speed', 'timestamp']
        
        # Color markers by speed
        taxi_df['color'] = taxi_df['speed'].apply(
            lambda s: '#ff0000' if s > 60 else 
                     '#ff8000' if s > 40 else 
                     '#ffff00' if s > 20 else '#00ff00'
        )
        
        # Add all taxis as markers
        map_fig.add_trace(go.Scattermap(
            lat=taxi_df['lat'],
            lon=taxi_df['lng'],
            mode='markers',
            marker=dict(
                size=10,
                color=taxi_df['color'],
                opacity=0.8
            ),
            customdata=taxi_df[['taxi_id', 'speed']],
            hovertemplate=(
                "<b>Taxi %{customdata[0]}</b><br>" +
                "Speed: %{customdata[1]:.0f} km/h<br>" +
                "Location: %{lat:.4f}, %{lon:.4f}<extra></extra>"
            ),
            name="All Taxis"
        ))
        
        # Highlight selected taxi if one is selected
        if selected_taxi_id and selected_taxi_id in taxi_data:
            selected_taxi = taxi_data[selected_taxi_id]
            map_fig.add_trace(go.Scattermap(
                lat=[selected_taxi['lat']],
                lon=[selected_taxi['lng']],
                mode='markers',
                marker=dict(
                    size=20,
                    color='#0000ff',
                    opacity=1
                ),
                customdata=[[selected_taxi_id, selected_taxi['speed']]],
                hovertemplate=(
                    "<b>Selected Taxi %{customdata[0]}</b><br>" +
                    "Speed: %{customdata[1]:.0f} km/h<br>" +
                    "Location: %{lat:.4f}, %{lon:.4f}<extra></extra>"
                ),
                name="Selected Taxi"
            ))
            
            # Add route for selected taxi
            route = get_route(selected_taxi_id)
            if route:
                route_lat = [p["latitude"] for p in route]
                route_lon = [p["longitude"] for p in route]
                map_fig.add_trace(go.Scattermap(
                    lat=route_lat,
                    lon=route_lon,
                    mode="lines",
                    line=dict(width=2, color='blue'),
                    name="Route",
                    hoverinfo='none'
                ))
    
    # Update map layout
    map_fig.update_layout(
        map_style="open-street-map",
        map=dict(
            center=dict(lat=39.9042, lon=116.4074),
            zoom=zoom_level
        ),
        margin={"r":0,"t":0,"l":0,"b":0},
        showlegend=False
    )
    
    # Calculate statistics
    active_taxis = len(taxi_data)
    violations = len(incident_log)
    
    # Calculate average speed
    avg_speed = 0
    if taxi_data:
        speeds = [data['speed'] for data in taxi_data.values()]
        avg_speed = sum(speeds) / len(speeds) if speeds else 0
    
    # Speed distribution chart
    speed_bins = [0] * 5
    if taxi_data:
        speeds = [data['speed'] for data in taxi_data.values()]
        speed_bins[0] = len([s for s in speeds if s < 20])
        speed_bins[1] = len([s for s in speeds if 20 <= s < 40])
        speed_bins[2] = len([s for s in speeds if 40 <= s < 60])
        speed_bins[3] = len([s for s in speeds if 60 <= s < 80])
        speed_bins[4] = len([s for s in speeds if s >= 80])
    
    speed_fig = go.Figure(
        go.Bar(
            x=['0-20', '20-40', '40-60', '60-80', '80+'],
            y=speed_bins,
            marker_color=['rgba(75, 192, 192, 0.6)', 'rgba(54, 162, 235, 0.6)', 
                         'rgba(255, 206, 86, 0.6)', 'rgba(255, 159, 64, 0.6)', 
                         'rgba(255, 99, 132, 0.6)']
        )
    )
    
    speed_fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)'),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    # Create incident cards
    incident_cards = []
    incidents_snapshot = list(incident_log)
    if incidents_snapshot:
        for incident in incidents_snapshot:
            card = dbc.Card(
                dbc.CardBody([
                    html.Div([
                        html.Div(
                            html.I(className="fas fa-exclamation-triangle"),
                            className="bg-danger bg-opacity-10 p-2 rounded-circle me-3"
                        ),
                        html.Div([
                            html.Div([
                                html.Strong(incident['type'], className="text-danger"),
                                html.Small(incident['timestamp'], className="text-muted ms-2")
                            ], className="d-flex justify-content-between"),
                            html.P(f"Taxi {incident['taxi_id']} exceeded speed limit", className="mb-1 small"),
                            html.Div([
                                html.Small(f"Speed: {incident['speed']:.0f} km/h", className="text-muted"),
                                html.Small("•", className="mx-2 text-muted"),
                                html.Small(f"Location: {incident['lat']:.4f}, {incident['lng']:.4f}", className="text-muted")
                            ], className="d-flex")
                        ], className="flex-grow-1")
                    ], className="d-flex")
                ], className="p-2"),
                className="mb-2 incident-card"
            )
            incident_cards.append(card)
    else:
        incident_cards = [
            html.Div("No incidents reported", className="text-center text-muted py-4")
        ]
    
    return (
        map_fig,
        active_taxis,
        violations,
        f"{avg_speed:.0f} km/h",
        speed_fig,
        incident_cards
    )

# Add this at the end of the `fetch_redis_data` function
print("Taxi Data:", taxi_data)

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)