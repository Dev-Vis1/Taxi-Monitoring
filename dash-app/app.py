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
from redis_client import get_all_taxi_ids, get_latest_location, get_route, get_all_taxi_distances, get_batch_locations, check_area_violations_bulk
import pytz


app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
    ]
)
server = app.server


MAPBOX_ACCESS_TOKEN = "pk.eyJ1Ijoic3VubWVldDU1IiwiYSI6ImNtY2ozMjBrMzA0dGkyaXNhM3Q0b3c1Z2IifQ.GPtW45RgkjnbsaGc2GOA3w"  
MAPBOX_STYLE = "outdoors"  

# Redis Configuration
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_DB = 0

violations_today = 0  
area_violations_today = 0  
total_distance_covered = 0.0  

taxi_data = {}
incident_log = deque(maxlen=500)

taxi_trajectories = {}  
MAX_TRAJECTORY_LENGTH = 10  
trajectory_update_interval = 1.0  

# Blacklist for taxi IDs 
BLACKLISTED_TAXIS = set()  
BLACKLISTED_INCIDENT_TYPES = set()

# Area violation configuration 
MONITORED_AREA = {
    'center_lat': 39.9163, 
    'center_lon': 116.3972,  
    'warning_radius_km': 10.0,  
    'max_radius_km': 15.0       
}

def is_in_monitored_area(lat, lon):
    """Check if coordinates are within the monitored area using the same logic as Flink"""
    
    import math
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat, lon, MONITORED_AREA['center_lat'], MONITORED_AREA['center_lon']])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance = 6371.0 * c  
    
    if distance <= MONITORED_AREA['warning_radius_km']:
        return "inside", distance
    elif distance <= MONITORED_AREA['max_radius_km']:
        return "warning", distance
    else:
        return "outside", distance

def is_blacklisted_incident(incident):
    if incident['taxi_id'] in BLACKLISTED_TAXIS:
        return True
    if incident['type'] in BLACKLISTED_INCIDENT_TYPES:
        return True
    if incident['type'] == 'Speed Violation' and incident['speed'] >= 200:
        return True
    if incident['type'] == 'Area Violation':  
        return False
    return False

# Real-time data fetcher
def fetch_simulation_data():
    global violations_today, area_violations_today, total_distance_covered
    print("=== Starting highly optimized real-time data fetcher thread ===")
    tz = pytz.timezone('Europe/Berlin')
    
    
    fetch_count = 0
    last_performance_log = time.time()

    taxi_ids_cache = []
    taxi_ids_cache_time = 0
    cache_duration = 30  
    
    previous_locations = {}
    
    while True:
        try:
            fetch_start = time.time()
            print(f"Fetching taxi data... (iteration {fetch_count})")
            
            current_time = time.time()
            if not taxi_ids_cache or (current_time - taxi_ids_cache_time) > cache_duration:
                taxi_ids_cache = get_all_taxi_ids()
                taxi_ids_cache_time = current_time
                print(f"Refreshed taxi IDs cache: {len(taxi_ids_cache)} taxi IDs")
            
            all_taxi_ids = taxi_ids_cache
            print(f"Using {len(all_taxi_ids)} taxi IDs from cache")
            
            # BATCH PROCESSING
            print("Fetching locations in bulk...")
            bulk_locations = get_batch_locations(all_taxi_ids, batch_size=500)
            
            new_taxi_data = {}
            processed_count = 0
            
            # Process all locations from bulk fetch
            for taxi_id, location in bulk_locations.items():
                if location["latitude"] != 0 and location["longitude"] != 0:
                    current_lat = location["latitude"]
                    current_lon = location["longitude"]
                    
                    update_taxi_trajectory(taxi_id, current_lat, current_lon, location["timestamp"])
                    
                    new_taxi_data[taxi_id] = {
                        'lat': current_lat,
                        'lng': current_lon,
                        'speed': location["speed"],
                        'timestamp': location["timestamp"] if location["timestamp"] else datetime.now(tz).strftime('%H:%M:%S')
                    }
                    processed_count += 1
                    status, distance = is_in_monitored_area(current_lat, current_lon)
                    if status == "warning":
                        if not any(i['taxi_id'] == taxi_id and i['type'] == 'Warning Radius'
                              for i in list(incident_log)[:5]):
                            incident_log.appendleft({
                            'taxi_id': taxi_id,
                            'type': 'Warning Radius',
                            'distance_from_center': distance,
                            'lat': current_lat,
                            'lng': current_lon,
                            'timestamp': datetime.now(tz).strftime('%H:%M:%S')
                        })
                        print(f"Warning: Taxi {taxi_id} in warning zone ({distance:.2f} km)")
                                    
            #  SPEED VIOLATION CHECK 
            for taxi_id, location in bulk_locations.items():
                speed = location["speed"]
                if speed > 50: 
                    incident = {
                        'taxi_id': taxi_id,
                        'type': 'Speed Violation',
                        'speed': speed,
                        'lat': location['latitude'],
                        'lng': location['longitude'],
                        'timestamp': datetime.now(tz).strftime('%H:%M:%S')
                    }
                    
                    if (not is_blacklisted_incident(incident) and 
                        not any(i['taxi_id'] == taxi_id and i['type'] == 'Speed Violation' 
                               for i in list(incident_log)[:3])): 
                        incident_log.appendleft(incident)
                        violations_today += 1
                        
            for taxi_id, data in new_taxi_data.items():
                status, distance = is_in_monitored_area(data['lat'], data['lng'])
                if status == "outside":
                    if not any(i['taxi_id'] == taxi_id and i['type'] == 'Area Violation'
                              for i in list(incident_log)[:5]):
                        incident_log.appendleft({
                            'taxi_id': taxi_id,
                            'type': 'Area Violation',
                            'distance_from_center': distance,
                            'lat': data['lat'],
                            'lng': data['lng'],
                            'timestamp': datetime.now(tz).strftime('%H:%M:%S')
                        })
                        area_violations_today += 1
                        print(f"Violation: Taxi {taxi_id} left monitored area ({distance:.2f} km)")
            
            total_distance_covered = get_all_taxi_distances()
            
            taxi_data.clear()
            taxi_data.update(new_taxi_data)
            
            # Performance logging
            fetch_time = time.time() - fetch_start
            fetch_count += 1
            
            if time.time() - last_performance_log > 30: 
                print(f"Performance: {processed_count}/{len(all_taxi_ids)} taxis processed in {fetch_time:.2f}s")
                print(f"Active taxis with valid locations: {len(taxi_data)}")
                print(f"Total distance covered: {total_distance_covered:.2f} km")
                print(f"Speed violations: {violations_today}, Area violations: {area_violations_today}")
                last_performance_log = time.time()
            
            time.sleep(1.5)  
        except Exception as e:
            print(f"Real-time data error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)

print("=== Starting simulation thread ===")
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
            
            dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        
                        html.H4("Taxi Activity Map", className="card-title mb-0 me-3"),
                        
                        html.Div([
                            html.Div([
                                # 1. Monitored Area switch first
                                html.Div([
                                    html.Span("Monitored Area:", className="me-2 small"),
                                    dbc.Switch(
                                        id="area-filter-switch",
                                        label=None,  
                                        value=False,
                                        className="d-inline-flex align-items-center me-3",
                                        style={"verticalAlign": "middle"}
                                    ),
                                ], className="d-inline-flex align-items-center"),
                                
                                # 2. Trajectories toggle
                                dbc.Button(
                                    [html.I(className="fas fa-route me-2"), "Trajectories"],
                                    id="toggle-trajectory",
                                    color="info",
                                    outline=True,
                                    className="me-3 btn-sm py-1"
                                ),
                                
                                # 3. Zoom controls 
                                dbc.ButtonGroup([
                                    dbc.Button(html.I(className="fas fa-search-plus"), 
                                            id="zoom-in", 
                                            color="light",
                                            className="px-2 py-1 btn-sm"),
                                    dbc.Button(html.I(className="fas fa-search-minus"), 
                                            id="zoom-out", 
                                            color="light",
                                            className="px-2 py-1 btn-sm"),
                                    dbc.Button(html.I(className="fas fa-crosshairs"), 
                                            id="reset-view", 
                                            color="light",
                                            className="px-2 py-1 btn-sm"),
                                ], className="me-0"),  # No right margin since it's last
                            ], className="d-flex align-items-center mb-2"),
                            
                            # Second row - taxi controls
                            html.Div([
                                html.Div([
                                    html.Span("Filter Taxis:", className="me-2 small"),
                                    dcc.Dropdown(
                                        id="taxi-limit",
                                        options=[
                                            {"label": "All", "value": -1},
                                            {"label": "5", "value": 5},
                                            {"label": "10", "value": 10},
                                            {"label": "15", "value": 15},
                                            {"label": "20", "value": 20},
                                            {"label": "30", "value": 30},
                                            {"label": "50", "value": 50}
                                        ],
                                        value=-1,
                                        style={"width": "100px"},
                                        clearable=False,
                                        className="me-3"
                                    ),
                                ], className="d-inline-flex align-items-center"),
                                
                               
                                dcc.Dropdown(
                                    id="taxi-id",
                                    options=[{"label": tid, "value": tid} for tid in get_all_taxi_ids()],
                                    placeholder="Select Taxi...",
                                    style={"width": "200px"},
                                    clearable=True,
                                )
                            ], className="d-flex align-items-center")
                        ], className="d-flex flex-column ms-auto", style={"width": "auto"})
                    ], className="d-flex align-items-center w-100")
                ], className="py-2"),
                
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
                                html.P("Total Distance", className="text-muted small mb-1"),
                                html.H3(id="total-distance", children="0 km", className="mb-0")
                            ]),
                            html.Div(html.I(className="fas fa-route fa-2x text-info"),
                                   className="bg-info bg-opacity-10 p-3 rounded-circle")
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ]), md=3)
            ], className="mb-4"),
            
            # Secondary stats row
            dbc.Row([
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
                    dcc.Dropdown(
                        id="incident-type-dropdown",
                        options=[
                            {"label": "All", "value": "All"},
                            {"label": "Speed Violation", "value": "Speed Violation"},
                            {"label": "Area Violation", "value": "Area Violation"},
                            {"label": "Warning Radius", "value": "Warning Radius"},
                        ],
                        value="All",
                        clearable=False,
                        style={"marginBottom": "10px"}
                    ),
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
            html.P("Team A6: Parthav Pillai, Sunmeet Kohli, Sanika Acharya, Matthew Ayodele",
            className="text-center w-100 mb-0 text-white"),
        ),
        color="dark",
        dark=True,
        className="mt-4 py-3"
    ),

    
    dcc.Interval(id='update-interval', interval=5000, n_intervals=0),  
    
    # Hidden div to store trajectory toggle state
    html.Div(id='trajectory-state', children='false', style={'display': 'none'}),
    html.Div(id='area-filter-state', children='false', style={'display': 'none'}),
])

# CSS 
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
        .map-controls .Select-control, 
        .map-controls .Select-input, 
        .map-controls .Select-menu-outer {
            height: 38px;
            vertical-align: middle;
        }
        .map-controls .Select-value {
            line-height: 38px !important;
        }
        .map-controls .Select-input > input {
            padding: 8px 0 12px !important;
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


@app.callback(
    Output('current-time', 'children'),
    Input('update-interval', 'n_intervals')
)
def update_time(n):
    tz = pytz.timezone('Europe/Berlin')
    now = datetime.now(tz)
    return now.strftime('%Y-%m-%d %H:%M:%S')

@app.callback(
    Output('area-filter-state', 'children'),
    [Input('area-filter-switch', 'value')]
)
def update_area_filter_state(switch_value):
    return 'true' if switch_value else 'false'

@app.callback(
    [Output('trajectory-state', 'children'),
     Output('toggle-trajectory', 'color'),
     Output('toggle-trajectory', 'outline')],
    [Input('toggle-trajectory', 'n_clicks')],
    [State('trajectory-state', 'children')]
)
def toggle_trajectory_display(n_clicks, current_state):
    if n_clicks is None:
        return 'false', 'info', True  
    
    new_state = 'true' if current_state == 'false' else 'false'
    
    if new_state == 'true':
        return 'true', 'info', False  
    else:
        return 'false', 'info', True 

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
     Input('taxi-id', 'value'),
     Input('trajectory-state', 'children'),
     Input('taxi-limit', 'value'),
     Input('area-filter-state', 'children'),
     Input('incident-type-dropdown', 'value')], 
    [State('taxi-map', 'relayoutData'),
     State('taxi-map', 'figure')]
)
def update_dashboard(n, zoom_in, zoom_out, reset_view, selected_taxi_id, show_trajectories, taxi_limit,area_filter_state,incident_type, relayout_data, current_figure):
    global violations_today, area_violations_today, total_distance_covered
    ctx = dash.callback_context
    start_time = time.time()
    zoom_level = 12
    
    if current_figure and 'layout' in current_figure and 'mapbox' in current_figure['layout']:
        current_mapbox = current_figure['layout']['mapbox']
        zoom_level = current_mapbox.get('zoom', zoom_level)
        center_lat = current_mapbox['center'].get('lat', 39.9042)
        center_lon = current_mapbox['center'].get('lon', 116.4074)
    else:
        center_lat = 39.9042
        center_lon = 116.4074
    
    # zoom controls
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
    
    map_fig = go.Figure()
    
    # Data snapshot 
    taxi_data_snapshot = taxi_data.copy() if taxi_data else {}
    if area_filter_state == 'true':

        filtered_taxi_data = {}

        for taxi_id, data in taxi_data_snapshot.items():

            status, _ = is_in_monitored_area(data['lat'], data['lng'])
            if status in ["inside", "warning"]:  
                filtered_taxi_data[taxi_id] = data
        taxi_data_snapshot = filtered_taxi_data
    
    
    if taxi_limit > 0 and len(taxi_data_snapshot) > taxi_limit:
        sorted_taxi_ids = sorted(taxi_data_snapshot.keys())
        limited_taxi_ids = sorted_taxi_ids[:taxi_limit]
        
        if selected_taxi_id and selected_taxi_id in taxi_data_snapshot and selected_taxi_id not in limited_taxi_ids:
            limited_taxi_ids = limited_taxi_ids[:-1] + [selected_taxi_id]
        
        taxi_data_snapshot = {tid: taxi_data_snapshot[tid] for tid in limited_taxi_ids}
        print(f"Debug: Limited display to {len(taxi_data_snapshot)} out of {len(taxi_data)} total taxis")

    if taxi_data_snapshot:
        print(f"Debug: Displaying {len(taxi_data_snapshot)} taxis with smooth trajectories")
        if show_trajectories == 'true':
            for taxi_id, trajectory in taxi_trajectories.items():
                if taxi_id in taxi_data_snapshot and len(trajectory) > 1:
                    lats = [pos['lat'] for pos in trajectory]
                    lons = [pos['lon'] for pos in trajectory]
                    
                    # Trajectory line
                    map_fig.add_trace(go.Scattermapbox(
                        lat=lats,
                        lon=lons,
                        mode='lines',
                        line=dict(width=3, color='rgba(86, 13, 175, 0.8)'),
                        hoverinfo='none',
                        showlegend=False,
                        name=f"Trajectory {taxi_id}"
                    ))
        
        
        taxi_positions = []
        current_time = time.time()
        
        for taxi_id, data in taxi_data_snapshot.items():
            smooth_lat, smooth_lon = get_smooth_taxi_position(taxi_id, current_time)
            if smooth_lat is not None and smooth_lon is not None:
                taxi_positions.append({
                    'taxi_id': taxi_id,
                    'lat': smooth_lat,
                    'lng': smooth_lon,
                    'speed': data['speed'],
                    'timestamp': data['timestamp']
                })
        
        if taxi_positions:
            taxi_df = pd.DataFrame(taxi_positions)
            
            taxi_df = taxi_df.dropna(subset=['lat', 'lng'])
            taxi_df = taxi_df[(taxi_df['lat'] != 0) & (taxi_df['lng'] != 0)]
           
            taxi_df['status'], taxi_df['distance'] = zip(*taxi_df.apply(lambda row: is_in_monitored_area(row['lat'], row['lng']), axis=1))
            
            # Taxi markers with smooth movement animation
            map_fig.add_trace(go.Scattermapbox(
                lat=taxi_df['lat'],
                lon=taxi_df['lng'],
                mode='markers',
                marker=dict(
                    size=20, 
                    color=taxi_df.apply(lambda row: 
                        '#ff0000' if row['status'] == 'outside' else
                        '#ffa500' if row['status'] == 'warning' else 
                        '#00ff00', axis=1),
                    opacity=1,  
                    symbol='car',
                    allowoverlap=True,
                    sizemode='diameter'
                ),
                customdata=taxi_df[['taxi_id', 'speed', 'status', 'distance']].values.tolist(),
                hovertemplate=(
                    "<b>Taxi %{customdata[0]}</b><br>"
                    "Speed: %{customdata[1]:.1f} km/h<br>"
                    "Location: %{lat:.4f}, %{lon:.4f}<br>"
                    "<i>Smooth interpolated position</i><extra></extra>"
                ),
                 hoverlabel=dict(
                    bgcolor=taxi_df.apply(lambda row: 
                      '#ff0000' if row['status'] == 'outside' else
                      '#ffa500' if row['status'] == 'warning' else 
                      '#00ff00', axis=1),
                    font=dict(color='white')
                 ),
                 
                name="Taxis with Trajectories",
                showlegend=False,
                hoverinfo='text'
            ))
        
        # Highlight selected taxi with Star marker  
        if selected_taxi_id and selected_taxi_id in taxi_data_snapshot:
            selected_lat, selected_lon = get_smooth_taxi_position(selected_taxi_id, current_time)
            if selected_lat is not None and selected_lon is not None:
                selected_taxi = taxi_data_snapshot[selected_taxi_id]
                map_fig.add_trace(go.Scattermapbox(
                    lat=[selected_lat],
                    lon=[selected_lon],
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
                        "Smooth Location: %{lat:.4f}, %{lon:.4f}<extra></extra>"
                    ),
                    name="Selected Taxi",
                    showlegend=False
                ))
            
            # Route for selected taxi
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
    
    # Monitored area boundary circle 15 km (red) and warning radius circle 10km (orange)
    import numpy as np
    circle_lat = []
    circle_lon = []
    for i in range(101):
        angle = 2 * np.pi * i / 100
        lat_offset = MONITORED_AREA['max_radius_km'] / 111.0  
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
    
    warning_circle_lat = []
    warning_circle_lon = []
    for i in range(101):
        angle = 2 * np.pi * i / 100
        lat_offset = MONITORED_AREA['warning_radius_km'] / 111.0
        lon_offset = MONITORED_AREA['warning_radius_km'] / (111.0 * np.cos(np.radians(MONITORED_AREA['center_lat'])))
        lat = MONITORED_AREA['center_lat'] + lat_offset * np.cos(angle)
        lon = MONITORED_AREA['center_lon'] + lon_offset * np.sin(angle)
        warning_circle_lat.append(lat)
        warning_circle_lon.append(lon)

    map_fig.add_trace(go.Scattermapbox(
        lat=warning_circle_lat,
        lon=warning_circle_lon,
        mode="lines",
        line=dict(width=2, color='rgba(0,0,0,0)'),
        name="Warning Radius (10km)",
        showlegend=False,
        hoverinfo='none'
    ))
    

    # Update map layout 
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
        dragmode='pan',  
        hovermode='closest'  
    )
    
    # Statistics calculation
    total_active_taxis = len(taxi_data) if taxi_data else 0
    displayed_taxis = len(taxi_data_snapshot)
    
    if taxi_limit > 0 and total_active_taxis > taxi_limit:
        active_taxis_display = f"{displayed_taxis}/{total_active_taxis}"
    else:
        active_taxis_display = total_active_taxis
    
    speed_violations = violations_today
    area_violations = area_violations_today
    total_distance = total_distance_covered
    
    # Calculate average speed
    if taxi_data_snapshot:
        total_speed = sum(data['speed'] for data in taxi_data_snapshot.values())
        avg_speed = total_speed / displayed_taxis
    else:
        avg_speed = 0

    # Speed distribution calculation
    speed_bins = [0, 0, 0, 0, 0] 
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
    
    # Incident cards 
    incident_cards = []
    recent_incidents = list(incident_log)[:10000]  
    if incident_type and incident_type != "All":
        recent_incidents = [i for i in recent_incidents if i.get('type') == incident_type]
        
    if recent_incidents:
        for incident in recent_incidents:
            if incident['type'] == 'Speed Violation':
                icon_class = "fas fa-tachometer-alt"
                icon_bg = "bg-danger"
                incident_text = f"Taxi {incident.get('taxi_id', '?')} exceeded speed limit ({incident.get('speed', 0):.0f} km/h)"
                location_text = f"Location: {incident.get('lat', 0):.4f}, {incident.get('lng', 0):.4f}"
            elif incident['type'] == 'Area Violation':
                icon_class = "fas fa-map-marked-alt"
                icon_bg = "bg-warning"
                incident_text = f"Taxi {incident.get('taxi_id', '?')} exited monitored area"
                location_text = f"Distance from center: {incident.get('distance_from_center', 0):.2f} km"
            elif incident['type'] == 'Warning Radius':
                icon_class = "fas fa-exclamation-circle"  
                icon_bg = "bg-warning"
                incident_text = f"Taxi {incident.get('taxi_id', '?')} entered warning zone (10-15km)"
                location_text = f"Distance from center: {incident.get('distance_from_center',0):.2f} km"
                
            else:
                icon_class = "fas fa-exclamation-triangle"
                icon_bg = "bg-danger"
                incident_text = f"Taxi {incident.get('taxi_id', '?')} - {incident.get('type', '?')}"
                location_text = f"Location: {incident.get('lat', 0):.4f}, {incident.get('lng', 0):.4f}"
            
            card = dbc.Card(
                dbc.CardBody([
                    html.Div([
                        html.Div(
                            html.I(className=icon_class, style={"fontSize": "1.5rem"}),
                            className=f"{icon_bg} bg-opacity-10 rounded-circle d-flex align-items-center justify-content-center shadow-sm",
                            style={"width": "48px", "height": "48px", "margin": "0 auto"}
                        ),
                        html.Div([
                            html.Div([
                                html.Strong(incident['type'],
                                            className="text-danger" if incident['type'] == 'Speed Violation'
                                            else "text-warning" if incident['type'] == 'Area Violation'
                                            else "text-primary"),
                                html.Small(incident['timestamp'], className="text-muted ms-2 fw-light")
                            ], className="d-flex justify-content-between align-items-center mb-1"),
                            html.P(incident_text, className="mb-1 small fw-semibold", style={"color": "#333"}),
                            html.Small(location_text, className="text-muted fst-italic")
                        ], className="flex-grow-1 ms-3")
                    ], className="d-flex align-items-center"),
                ], className="p-2"),
                className="mb-2 incident-card border-0 shadow-sm"
            )
            incident_cards.append(card)
    else:
        incident_cards = html.Div("No incidents reported", className="text-center text-muted py-4")
    
    # Performance logging
    processing_time = time.time() - start_time
    if processing_time > 1.0: 
        print(f"Dashboard update took {processing_time:.2f}s for {displayed_taxis} displayed / {total_active_taxis} total taxis")
    
    return (
        map_fig,
        active_taxis_display,
        speed_violations,
        area_violations,
        f"{total_distance:.1f} km",
        f"{avg_speed:.0f} km/h",
        speed_fig,
        incident_cards
    )

def interpolate_position(pos1, pos2, timestamp1, timestamp2, current_time):
    """
    Interpolate position between two points based on time for smooth movement
    Returns interpolated lat, lon based on current time
    """
    import datetime
    
    try:
        if isinstance(timestamp1, str):
            t1 = datetime.datetime.strptime(timestamp1, '%H:%M:%S')
            t1 = t1.timestamp()
        else:
            t1 = timestamp1
            
        if isinstance(timestamp2, str):
            t2 = datetime.datetime.strptime(timestamp2, '%H:%M:%S')
            t2 = t2.timestamp()
        else:
            t2 = timestamp2
            
        if isinstance(current_time, str):
            ct = datetime.datetime.strptime(current_time, '%H:%M:%S')
            ct = ct.timestamp()
        else:
            ct = current_time
            
        # Calculate interpolation factor
        if t2 == t1:
            factor = 0
        else:
            factor = (ct - t1) / (t2 - t1)
            factor = max(0, min(1, factor))  
        
        lat = pos1[0] + (pos2[0] - pos1[0]) * factor
        lon = pos1[1] + (pos2[1] - pos1[1]) * factor
        
        return lat, lon
    except:
        return pos2[0], pos2[1]

def update_taxi_trajectory(taxi_id, lat, lon, timestamp):
    """Update taxi trajectory with new position for smooth movement"""
    if taxi_id not in taxi_trajectories:
        taxi_trajectories[taxi_id] = deque(maxlen=MAX_TRAJECTORY_LENGTH)
    
    taxi_trajectories[taxi_id].append({
        'lat': lat,
        'lon': lon,
        'timestamp': timestamp,
        'time_added': time.time()
    })

def get_smooth_taxi_position(taxi_id, current_time=None):
    """
    Get smooth interpolated position for a taxi based on its trajectory
    This creates the Uber-like smooth movement effect
    """
    if taxi_id not in taxi_trajectories or len(taxi_trajectories[taxi_id]) < 2:
        if taxi_id in taxi_data:
            return taxi_data[taxi_id]['lat'], taxi_data[taxi_id]['lng']
        return None, None
    
    trajectory = list(taxi_trajectories[taxi_id])
    pos1 = trajectory[-2]
    pos2 = trajectory[-1]
    
    if current_time is None:
        current_time = time.time()
    
   
    coord1 = (pos1['lat'], pos1['lon'])
    coord2 = (pos2['lat'], pos2['lon'])
    
    lat, lon = interpolate_position(
        coord1, coord2, 
        pos1.get('time_added', current_time), 
        pos2.get('time_added', current_time), 
        current_time
    )
    
    return lat, lon

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)