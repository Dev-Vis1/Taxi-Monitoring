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
from redis_client import get_all_taxi_ids, get_latest_location, get_route, get_all_taxi_distances
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
area_violations_today = 0  # New: Track area violations
total_distance_covered = 0.0  # New: Track total distance covered by all taxis

taxi_data = {}
incident_log = deque(maxlen=20)

# Blacklist for taxi IDs or incident types
BLACKLISTED_TAXIS = set()  
BLACKLISTED_INCIDENT_TYPES = set()

# Area violation configuration (Beijing city center)
MONITORED_AREA = {
    'center_lat': 39.9042,
    'center_lon': 116.4074,
    'warning_radius_km': 10.0,  # Warning when taxi approaches boundary
    'max_radius_km': 15.0       # Violation when taxi exits this area
}

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth (in kilometers)"""
    import math
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r

def is_in_monitored_area(lat, lon):
    """Check if coordinates are within the monitored area"""
    distance = haversine_distance(lat, lon, MONITORED_AREA['center_lat'], MONITORED_AREA['center_lon'])
    return distance <= MONITORED_AREA['max_radius_km'], distance 

def is_blacklisted_incident(incident):
    if incident['taxi_id'] in BLACKLISTED_TAXIS:
        return True
    if incident['type'] in BLACKLISTED_INCIDENT_TYPES:
        return True
    if incident['type'] == 'Speed Violation' and incident['speed'] >= 200:
        return True
    if incident['type'] == 'Area Violation':  # Don't blacklist area violations
        return False
    return False

# Real-time data fetcher using current locations and calculated speeds - HIGHLY OPTIMIZED
def fetch_simulation_data():
    global violations_today, area_violations_today, total_distance_covered
    print("=== Starting highly optimized real-time data fetcher thread ===")
    tz = pytz.timezone('Europe/Berlin')
    
    # Performance tracking
    fetch_count = 0
    last_performance_log = time.time()
    
    # Caching for better performance
    taxi_ids_cache = []
    taxi_ids_cache_time = 0
    cache_duration = 30  # Cache taxi IDs for 30 seconds
    
    # Store previous locations for distance calculation
    previous_locations = {}
    
    while True:
        try:
            fetch_start = time.time()
            print(f"Fetching taxi data... (iteration {fetch_count})")
            
            # Use cached taxi IDs if available and fresh
            current_time = time.time()
            if not taxi_ids_cache or (current_time - taxi_ids_cache_time) > cache_duration:
                taxi_ids_cache = get_all_taxi_ids()
                taxi_ids_cache_time = current_time
                print(f"Refreshed taxi IDs cache: {len(taxi_ids_cache)} taxi IDs")
            
            all_taxi_ids = taxi_ids_cache
            print(f"Using {len(all_taxi_ids)} taxi IDs from cache")
            
            # Clear old data and get current real-time positions
            new_taxi_data = {}
            
            # Process taxis in optimized batches
            batch_size = 200  # Increased batch size for better efficiency
            processed_count = 0
            
            for i in range(0, len(all_taxi_ids), batch_size):
                batch_taxi_ids = all_taxi_ids[i:i + batch_size]
                
                for taxi_id in batch_taxi_ids:
                    # Get current location from Redis
                    location = get_latest_location(taxi_id)
                    if location and location["latitude"] != 0 and location["longitude"] != 0:
                        current_lat = location["latitude"]
                        current_lon = location["longitude"]
                        
                        new_taxi_data[taxi_id] = {
                            'lat': current_lat,
                            'lng': current_lon,
                            'speed': location["speed"],
                            'timestamp': location["timestamp"] if location["timestamp"] else datetime.now(tz).strftime('%H:%M:%S')
                        }
                        processed_count += 1
                        
                        # Check for area violations
                        is_in_area, distance_from_center = is_in_monitored_area(current_lat, current_lon)
                        if not is_in_area:
                            incident = {
                                'taxi_id': taxi_id,
                                'type': 'Area Violation',
                                'distance_from_center': distance_from_center,
                                'lat': current_lat,
                                'lng': current_lon,
                                'timestamp': datetime.now(tz).strftime('%H:%M:%S')
                            }
                            # Check if this area violation is already logged recently
                            if not any(i['taxi_id'] == taxi_id and i['type'] == 'Area Violation' 
                                     for i in list(incident_log)[:5]):
                                incident_log.appendleft(incident)
                                area_violations_today += 1
                        
                        # Optimized speed violation detection (only check high speeds)
                        speed = location["speed"]
                        if speed > 60:  # Only process potential violations
                            incident = {
                                'taxi_id': taxi_id,
                                'type': 'Speed Violation',
                                'speed': speed,
                                'lat': current_lat,
                                'lng': current_lon,
                                'timestamp': datetime.now(tz).strftime('%H:%M:%S')
                            }
                            # Quick blacklist check and duplicate prevention
                            if (not is_blacklisted_incident(incident) and 
                                not any(i['taxi_id'] == taxi_id and i['type'] == 'Speed Violation' 
                                       for i in list(incident_log)[:3])):  # Check only last 3
                                incident_log.appendleft(incident)
                                violations_today += 1
            
            # Update total distance covered from Flink calculations
            total_distance_covered = get_all_taxi_distances()
            
            # Update taxi_data atomically for thread safety
            taxi_data.clear()
            taxi_data.update(new_taxi_data)
            
            # Performance logging
            fetch_time = time.time() - fetch_start
            fetch_count += 1
            
            if time.time() - last_performance_log > 30:  # Log every 30 seconds
                print(f"Performance: {processed_count}/{len(all_taxi_ids)} taxis processed in {fetch_time:.2f}s")
                print(f"Active taxis with valid locations: {len(taxi_data)}")
                print(f"Total distance covered: {total_distance_covered:.2f} km")
                print(f"Speed violations: {violations_today}, Area violations: {area_violations_today}")
                last_performance_log = time.time()
            
            time.sleep(3)  # Optimized sleep time
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
            # Stats cards - Modified to move "Total Distance" to secondary row and "Avg. Speed" to primary row
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
                ]), md=3),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Speed Violations", className="text-muted small mb-1"),
                                html.H3(id="violations-today", children="0", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-exclamation-triangle fa-2x text-danger"),
                                className="bg-danger bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=3),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Area Violations", className="text-muted small mb-1"),
                                html.H3(id="area-violations", children="0", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-map-marked-alt fa-2x text-warning"),
                                className="bg-warning bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=3),
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
                ]), md=3)
            ], className="mb-4"),

            # Secondary stats row - Now with "Total Distance" in first position
            dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Total Distance", className="text-muted small mb-1"),
                                html.H3(id="total-distance", children="0 km", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-route fa-2x text-info"),
                                className="bg-info bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=4),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("Monitored Area", className="text-muted small mb-1"),
                                html.H4(f"±{MONITORED_AREA['max_radius_km']} km", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-circle-notch fa-2x text-secondary"),
                                className="bg-secondary bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=4),
                dbc.Col(dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.P("System Status", className="text-muted small mb-1"),
                                html.H4("ONLINE", className="mb-0 text-success")
                            ]),
                            html.Div(html.I(className="fas fa-check-circle fa-2x text-success"),
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
            html.P("Beijing Taxi Monitoring System ©️ 2023 | Real-time Stream Processing Dashboard",
                   className="text-center w-100 mb-0"),
        ),
        color="dark",
        dark=True,
        className="mt-4 py-3"
    ),

    # Interval for updates - HIGHLY OPTIMIZED for better performance
    dcc.Interval(id='update-interval', interval=15000, n_intervals=0),  # Increased to 15 seconds for better performance
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
     Output('area-violations', 'children'),
     Output('total-distance', 'children'),
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
    global violations_today, area_violations_today, total_distance_covered
    ctx = dash.callback_context
    
    # Performance optimization: cache frequently used values
    start_time = time.time()
    
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
    
    # Optimized data snapshot - avoid deep copy for performance
    taxi_data_snapshot = taxi_data.copy() if taxi_data else {}

    if taxi_data_snapshot:
        # Debug: Print what taxis we have
        print(f"Debug: Displaying {len(taxi_data_snapshot)} taxis")
        
        # Convert to DataFrame for efficient processing - NO ARTIFICIAL CAP
        taxi_df = pd.DataFrame.from_dict(taxi_data_snapshot, orient='index').reset_index()
        # Real-time data has 4 fields: taxi_id, lat, lng, speed, timestamp
        taxi_df.columns = ['taxi_id', 'lat', 'lng', 'speed', 'timestamp']
        
        # Optimize by filtering out invalid data points
        taxi_df = taxi_df.dropna(subset=['lat', 'lng'])
        taxi_df = taxi_df[(taxi_df['lat'] != 0) & (taxi_df['lng'] != 0)]
   
        # Color markers by speed and area status for better visualization
        def get_speed_color(speed):
            if speed > 80: return '#ff0000'      # Red - Very fast
            elif speed > 60: return '#ff4500'   # Orange-red - Fast  
            elif speed > 40: return '#ffa500'   # Orange - Medium-fast
            elif speed > 20: return '#ffff00'   # Yellow - Medium
            else: return '#00ff00'              # Green - Slow
        
        def get_area_status_color(lat, lng):
            """Get color based on area status"""
            is_in_area, distance = is_in_monitored_area(lat, lng)
            if not is_in_area:
                return '#ff0000'  # Red - Outside area
            elif distance > MONITORED_AREA['warning_radius_km']:
                return '#ffa500'  # Orange - Near boundary
            else:
                return None  # Use speed color
        
        # Apply colors based on area status first, then speed
        taxi_df['area_color'] = taxi_df.apply(lambda row: get_area_status_color(row['lat'], row['lng']), axis=1)
        taxi_df['speed_color'] = taxi_df['speed'].apply(get_speed_color)
        taxi_df['color'] = taxi_df['area_color'].fillna(taxi_df['speed_color'])
        
        # Use highly optimized scatter plot for better performance with many points
        map_fig.add_trace(go.Scattermapbox(
            lat=taxi_df['lat'],
            lon=taxi_df['lng'],
            mode='markers',
            marker=dict(
                size=18,  # Smaller markers for better performance with many taxis
                color=taxi_df['color'],
                opacity=0.8,  # More transparent for better layering
                symbol='car',
                allowoverlap=True,
                sizemode='diameter'  # More efficient sizing
            ),
            customdata=taxi_df[['taxi_id', 'speed']],
            hovertemplate=(
                "<b>Taxi %{customdata[0]}</b><br>"
                "Speed: %{customdata[1]:.1f} km/h<br>"
                "Location: %{lat:.4f}, %{lon:.4f}<extra></extra>"
            ),
            name="Taxis",
            showlegend=False,
            text=None,  # Remove text for better performance
            hoverinfo='text'  # Optimize hover info
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
                showlegend=False                ))
            
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
    
    # Add monitored area boundary circle
    import numpy as np
    circle_lat = []
    circle_lon = []
    for i in range(101):
        angle = 2 * np.pi * i / 100
        # Approximate circle in lat/lon (not perfect due to projection)
        lat_offset = MONITORED_AREA['max_radius_km'] / 111.0  # Rough conversion
        lon_offset = MONITORED_AREA['max_radius_km'] / (111.0 * np.cos(np.radians(MONITORED_AREA['center_lat'])))
        lat = MONITORED_AREA['center_lat'] + lat_offset * np.cos(angle)
        lon = MONITORED_AREA['center_lon'] + lon_offset * np.sin(angle)
        circle_lat.append(lat)
        circle_lon.append(lon)
    
    map_fig.add_trace(go.Scattermapbox(
        lat=circle_lat,
        lon=circle_lon,
        mode="lines",
        line=dict(width=2, color='rgba(255,0,0,0.5)'),
        name="Monitored Area Boundary",
        showlegend=False,
        hoverinfo='none'
    ))

    # Update map layout with optimized settings
    map_fig.update_layout(
        mapbox=dict(
            accesstoken=MAPBOX_ACCESS_TOKEN,
            style=MAPBOX_STYLE,
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom_level
        ),
        margin={"r":0,"t":0,"l":0,"b":0},
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        dragmode='pan',  # Optimize for panning
        hovermode='closest'  # Optimize hover detection
    )
    
    # Optimized statistics calculation
    active_taxis = len(taxi_data_snapshot)
    speed_violations = violations_today
    area_violations = area_violations_today
    total_distance = total_distance_covered
    
    # Calculate average speed more efficiently
    if taxi_data_snapshot:
        total_speed = sum(data['speed'] for data in taxi_data_snapshot.values())
        avg_speed = total_speed / active_taxis
    else:
        avg_speed = 0

    # Optimized speed distribution calculation
    speed_bins = [0, 0, 0, 0, 0]  # Pre-allocate
    if taxi_data_snapshot:
        for data in taxi_data_snapshot.values():
            speed = data['speed']
            if speed < 20:
                speed_bins[0] += 1
            elif speed < 40:
                speed_bins[1] += 1
            elif speed < 60:
                speed_bins[2] += 1
            elif speed < 80:
                speed_bins[3] += 1
            else:
                speed_bins[4] += 1
    
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
    
    # Optimized incident cards generation - Enhanced for different violation types
    incident_cards = []
    recent_incidents = list(incident_log)[:10]  # Only show last 10 incidents for performance
    
    if recent_incidents:
        for incident in recent_incidents:
            # Different styling for different incident types
            if incident['type'] == 'Speed Violation':
                icon_class = "fas fa-tachometer-alt"
                icon_bg = "bg-danger"
                incident_text = f"Taxi {incident['taxi_id']} exceeded speed limit ({incident['speed']:.0f} km/h)"
                location_text = f"Location: {incident['lat']:.4f}, {incident['lng']:.4f}"
            elif incident['type'] == 'Area Violation':
                icon_class = "fas fa-map-marked-alt"
                icon_bg = "bg-warning"
                incident_text = f"Taxi {incident['taxi_id']} exited monitored area"
                location_text = f"Distance from center: {incident['distance_from_center']:.2f} km"
            else:
                icon_class = "fas fa-exclamation-triangle"
                icon_bg = "bg-danger"
                incident_text = f"Taxi {incident['taxi_id']} - {incident['type']}"
                location_text = f"Location: {incident['lat']:.4f}, {incident['lng']:.4f}"
            
            card = dbc.Card(
                dbc.CardBody([
                    html.Div([
                        html.Div(
                            html.I(className=icon_class),
                             className=f"{icon_bg} bg-opacity-10 p-2 rounded-circle d-flex align-items-center justify-content-center",
                             style={"width": "40px", "height": "40px"}
                             
                        ),
                        html.Div([
                            html.Div([
                                html.Strong(incident['type'], className="text-danger" if incident['type'] == 'Speed Violation' else "text-warning"),
                                html.Small(incident['timestamp'], className="text-muted ms-2")
                            ], className="d-flex justify-content-between"),
                            html.P(incident_text, className="mb-1 small"),
                            html.Div([
                                html.Small(location_text, className="text-muted")
                            ], className="d-flex")
                        ], className="flex-grow-1")
                    ], className="d-flex")
                ], className="p-2"),
                className="mb-2 incident-card"
            )
            incident_cards.append(card)
    else:
        incident_cards = html.Div("No incidents reported", className="text-center text-muted py-4")
    
    # Performance logging
    processing_time = time.time() - start_time
    if processing_time > 1.0:  # Log slow updates
        print(f"Dashboard update took {processing_time:.2f}s for {active_taxis} taxis")
    
    return (
        map_fig,
        active_taxis,
        speed_violations,
        area_violations,
        f"{total_distance:.1f} km",
        f"{avg_speed:.0f} km/h",
        speed_fig,
        incident_cards
    )


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)