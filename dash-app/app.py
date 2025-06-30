import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime
import redis
from threading import Thread
from collections import deque
import time
from redis_client import get_all_taxi_ids, get_latest_location, get_route
import pytz

# Initialize the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
    ]
)
server = app.server

# Mapbox Configuration (REPLACE WITH YOUR TOKEN)
MAPBOX_ACCESS_TOKEN = "pk.eyJ1Ijoic3VubWVldDU1IiwiYSI6ImNtY2ozMjBrMzA0dGkyaXNhM3Q0b3c1Z2IifQ.GPtW45RgkjnbsaGc2GOA3w"  # Get from mapbox.com
MAPBOX_STYLE = "streets"  # Other options: "light", "dark", "outdoors", "satellite"

# Redis Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_DB = 0

# Data storage
violations_today = 0  

taxi_data = {}
incident_log = deque(maxlen=20)

# Blacklist for taxi IDs or incident types
BLACKLISTED_TAXIS = set()  
BLACKLISTED_INCIDENT_TYPES = set() 

def is_blacklisted_incident(incident):
    if incident['taxi_id'] in BLACKLISTED_TAXIS:
        return True
    if incident['type'] in BLACKLISTED_INCIDENT_TYPES:
        return True
    if incident['type'] == 'Speed Violation' and incident['speed'] >= 200:
        return True
    return False

# Real-time data fetcher using current locations and calculated speeds
def fetch_simulation_data():
    global violations_today
    print("=== Starting real-time data fetcher thread ===")
    tz = pytz.timezone('Europe/Berlin')
    while True:
        try:
            print("Fetching taxi data...")
            # Get all taxi IDs and their current locations
            all_taxi_ids = get_all_taxi_ids()
            print(f"Found {len(all_taxi_ids)} taxi IDs")
            
            # Clear old data and get current real-time positions
            taxi_data.clear()
            
            for taxi_id in all_taxi_ids:
                # Get current location from Redis
                location = get_latest_location(taxi_id)
                if location:
                    taxi_data[taxi_id] = {
                        'lat': location["latitude"],
                        'lng': location["longitude"],
                        'speed': location["speed"],
                        'timestamp': location["timestamp"] if location["timestamp"] else datetime.now(tz).strftime('%H:%M:%S')
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
                            'timestamp': datetime.now(tz).strftime('%H:%M:%S')
                        }
                        # Blacklist check
                        if not is_blacklisted_incident(incident):
                            # Check if this incident is already logged
                            if not any(i['taxi_id'] == taxi_id and i['speed'] == speed and i['timestamp'] == incident['timestamp'] for i in incident_log):
                                incident_log.appendleft(incident)
                                violations_today += 1  # Increment total violations
            time.sleep(1)  # Poll every second
        except Exception as e:
            print(f"Real-time data error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)

print("=== Starting simulation thread ===")
# Start simulation fetcher thread
simulation_thread = Thread(target=fetch_simulation_data, daemon=True)
simulation_thread.start()
print("=== Simulation thread started ===")

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
                    html.Span(className="pulse-dot me-2", style={"backgroundColor": "#00ff00", "width": "10px", "height": "10px", "borderRadius": "50%", "display": "inline-block", "marginRight": "8px", "animation": "pulse 1.5s infinite"}),
                    "Real-time"
                ], color="primary", className="me-3 d-flex align-items-center"),

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
                        dbc.ButtonGroup([
                            dbc.Button(html.I(className="fas fa-search-plus"), id="zoom-in", color="light"),
                            dbc.Button(html.I(className="fas fa-search-minus"), id="zoom-out", color="light"),
                            dbc.Button(html.I(className="fas fa-crosshairs"), id="reset-view", color="light"),
                        ], className="me-2"),
                        dcc.Dropdown(
                            id="taxi-id",
                            options=[{"label": tid, "value": tid} for tid in get_all_taxi_ids()],
                            placeholder="Select Taxi to Track",
                            style={"width": "300px"},
                            clearable=True
                        )
                    ], className="float-end")
                ], className="d-flex justify-content-between align-items-center"),
                dcc.Graph(
                    id='taxi-map',
                    config={'scrollZoom': True, 'displayModeBar': False},
                    style={'height': '500px'}
                )
            ], className="mb-4"),

            # Stats cards
            dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Active Taxis", className="text-muted small mb-1"),
                                html.H3(id="active-taxis", children="0", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-taxi fa-2x text-primary"),
                                   className="bg-primary bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=4),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Violations Today", className="text-muted small mb-1"),
                                html.H3(id="violations-today", children="0", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-exclamation-triangle fa-2x text-danger"),
                                   className="bg-danger bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=4),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Avg. Speed", className="text-muted small mb-1"),
                                html.H3(id="avg-speed", children="0 km/h", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-tachometer-alt fa-2x text-success"),
                                   className="bg-success bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=4)
            ], className="mb-4")
        ]),

        # Right column
        dbc.Col(md=4, children=[
            dbc.Card([
                dbc.CardHeader([
                    html.I(className="fas fa-exclamation-circle text-danger me-2"),
                    html.H4("Recent Incidents", className="d-inline-block mb-0")
                ]),
                dbc.CardBody([
                    html.Div(id="incidents-container", className="overflow-auto", style={"maxHeight": "400px"})
                ])
            ], className="mb-4"),
            dbc.Card([
                dbc.CardHeader(html.H4("Speed Distribution", className="card-title")),
                dbc.CardBody([
                    dcc.Graph(id="speed-chart", config={'displayModeBar': False}, style={'height': '200px'})
                ])
            ])
        ])
    ], className="mt-4"),

    # Footer
    dbc.Navbar(
        dbc.Container(
            html.P("Beijing Taxi Monitoring System © 2023 | Real-time Stream Processing Dashboard",
                   className="text-center w-100 mb-0"),
        ),
        color="dark",
        dark=True,
        className="mt-4 py-3"
    ),

    # Interval for updates
    dcc.Interval(id='update-interval', interval=5000, n_intervals=0),
])

# CSS styles
app.css.append_css({
    'external_url': 'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap'
})
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
    '''
})

# Callbacks
@app.callback(
    Output('current-time', 'children'),
    Input('update-interval', 'n_intervals')
)
def update_time(n):
    tz = pytz.timezone('Europe/Berlin')
    now = datetime.now(tz)
    return now.strftime('%Y-%m-%d %H:%M:%S')

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
    [State('taxi-map', 'relayoutData'),
     State('taxi-map', 'figure')]
)
def update_dashboard(n, zoom_in, zoom_out, reset_view, selected_taxi_id, relayout_data, current_figure):
    global violations_today
    ctx = dash.callback_context
    
    # Default values
    zoom_level = 12
    
    # Get current map parameters from the figure if available
    if current_figure and 'layout' in current_figure and 'mapbox' in current_figure['layout']:
        current_mapbox = current_figure['layout']['mapbox']
        zoom_level = current_mapbox.get('zoom', zoom_level)
        center_lat = current_mapbox['center'].get('lat', 39.9042)
        center_lon = current_mapbox['center'].get('lon', 116.4074)
    else:
        center_lat = 39.9042
        center_lon = 116.4074
    
    # Handle zoom controls
    if ctx.triggered:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if trigger_id == 'zoom-in':
            zoom_level += 1
        elif trigger_id == 'zoom-out':
            zoom_level -= 1
        elif trigger_id == 'reset-view':
            zoom_level = 12
            center_lat = 39.9042
            center_lon = 116.4074
    
    # Create map figure
    map_fig = go.Figure()
    
    # Add taxi markers if we have data - Optimized for high performance
    taxi_data_snapshot = dict(taxi_data) if taxi_data else {}

    if taxi_data_snapshot:
        # Debug: Print what taxis we have
        print(f"Debug: Displaying {len(taxi_data_snapshot)} taxis")
        
        # Convert to DataFrame for efficient processing
        taxi_df = pd.DataFrame.from_dict(taxi_data_snapshot, orient='index').reset_index()
        # Real-time data has 4 fields: taxi_id, lat, lng, speed, timestamp
        taxi_df.columns = ['taxi_id', 'lat', 'lng', 'speed', 'timestamp']
   
        # Color markers by speed for better visualization
        def get_speed_color(speed):
            if speed > 80: return '#ff0000'      # Red - Very fast
            elif speed > 60: return '#ff4500'   # Orange-red - Fast  
            elif speed > 40: return '#ffa500'   # Orange - Medium-fast
            elif speed > 20: return '#ffff00'   # Yellow - Medium
            else: return '#00ff00'              # Green - Slow
        
        taxi_df['color'] = taxi_df['speed'].apply(get_speed_color)
        
        # Use scatter plot for better performance with many points
        map_fig.add_trace(go.Scattermapbox(
            lat=taxi_df['lat'],
            lon=taxi_df['lng'],
            mode='markers',
            marker=dict(
                size=12,  # Smaller size for better performance with many taxis
                color=taxi_df['color'],
                opacity=0.8,
                symbol='circle',
                allowoverlap=True
            ),
            customdata=taxi_df[['taxi_id', 'speed']],
            hovertemplate=(
                "<b>Taxi %{customdata[0]}</b><br>"
                "Speed: %{customdata[1]:.1f} km/h<br>"
                "Location: %{lat:.4f}, %{lon:.4f}<extra></extra>"
            ),
            name="Taxis",
            showlegend=False
        ))
        
        # Highlight selected taxi with special marker
        if selected_taxi_id and selected_taxi_id in taxi_data_snapshot:
            selected_taxi = taxi_data_snapshot[selected_taxi_id]
            map_fig.add_trace(go.Scattermapbox(
                lat=[selected_taxi['lat']],
                lon=[selected_taxi['lng']],
                mode='markers',
                marker=dict(
                    size=20,
                    color='#0000ff',
                    opacity=1,
                    symbol='star',
                    allowoverlap=True
                ),
                customdata=[[selected_taxi_id, selected_taxi['speed']]],
                hovertemplate=(
                    "<b>SELECTED: Taxi %{customdata[0]}</b><br>"
                    "Speed: %{customdata[1]:.1f} km/h<br>"
                    "Location: %{lat:.4f}, %{lon:.4f}<extra></extra>"
                ),
                name="Selected Taxi",
                showlegend=False
            ))
            
            # Add route for selected taxi
            route = get_route(selected_taxi_id)
            if route:
                route_lat = [p["latitude"] for p in route]
                route_lon = [p["longitude"] for p in route]
                map_fig.add_trace(go.Scattermapbox(
                    lat=route_lat,
                    lon=route_lon,
                    mode="lines",
                    line=dict(width=3, color='rgba(0,0,255,0.6)'),
                    name="Route",
                    hoverinfo='none',
                    showlegend=False
                ))

    # Update map layout with Mapbox
    map_fig.update_layout(
        mapbox=dict(
            accesstoken=MAPBOX_ACCESS_TOKEN,
            style=MAPBOX_STYLE,
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom_level
        ),
        margin={"r":0,"t":0,"l":0,"b":0},
        showlegend=False
    )
    
    # Calculate statistics
    active_taxis = len(taxi_data_snapshot)
    violations = violations_today  # Use the total violations counter
    avg_speed = sum(data['speed'] for data in taxi_data_snapshot.values())/len(taxi_data_snapshot) if taxi_data_snapshot else 0

    # Speed distribution chart
    speed_bins = [0]*5
    if taxi_data_snapshot:
        speeds = [data['speed'] for data in taxi_data_snapshot.values()]
        speed_bins = [
            len([s for s in speeds if s < 20]),
            len([s for s in speeds if 20 <= s < 40]),
            len([s for s in speeds if 40 <= s < 60]),
            len([s for s in speeds if 60 <= s < 80]),
            len([s for s in speeds if s >= 80])
        ]
    
    speed_fig = go.Figure(go.Bar(
        x=['0-20', '20-40', '40-60', '60-80', '80+'],
        y=speed_bins,
        marker_color=['rgba(75, 192, 192, 0.6)', 'rgba(54, 162, 235, 0.6)', 
                     'rgba(255, 206, 86, 0.6)', 'rgba(255, 159, 64, 0.6)', 
                     'rgba(255, 99, 132, 0.6)']
    ))
    speed_fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        yaxis=dict(showgrid=True, gridcolor='rgba(0,0,0,0.1)'),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    # Create incident cards
    incident_cards = []
    if incident_log:
        for incident in list(incident_log):
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
                                html.Small(f"Location: {incident['lat']:.4f}, {incident['lng']:.4f}", className="text-muted ms-2")
                            ], className="d-flex")
                        ], className="flex-grow-1")
                    ], className="d-flex")
                ], className="p-2"),
                className="mb-2 incident-card"
            )
            incident_cards.append(card)
    else:
        incident_cards = html.Div("No incidents reported", className="text-center text-muted py-4")
    
    return (
        map_fig,
        active_taxis,
        violations,
        f"{avg_speed:.0f} km/h",
        speed_fig,
        incident_cards
    )


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)