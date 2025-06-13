# app.py
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from redis_client import get_all_taxi_ids, get_latest_location, get_route

app = dash.Dash(__name__)
server = app.server

# Initial taxi IDs (could be dynamic later)
taxi_ids = get_all_taxi_ids()

app.layout = html.Div([
    html.H1("Real-Time Taxi Tracker", style={"textAlign": "center"}),

    dcc.Dropdown(
        id="taxi-id",
        options=[{"label": tid, "value": tid} for tid in taxi_ids],
        placeholder="Select a Taxi ID",
        style={"width": "50%", "margin": "auto"}
    ),

    dcc.Graph(id="map-plot"),
    
    dcc.Interval(
        id="refresh",
        interval=5*1000,  # 5 seconds
        n_intervals=0
    )
])

@app.callback(
    Output("map-plot", "figure"),
    [Input("taxi-id", "value"), Input("refresh", "n_intervals")]
)
def update_map(taxi_id, _):
    if not taxi_id:
        # Default empty map
        return px.scatter_mapbox(zoom=10).update_layout(mapbox_style="open-street-map")

    latest = get_latest_location(taxi_id)
    route = get_route(taxi_id)

    if not latest:
        # Log missing data for debugging
        print(f"No latest location found for Taxi ID: {taxi_id}")
        return px.scatter_mapbox(zoom=10).update_layout(mapbox_style="open-street-map")

    # Create map with latest location
    fig = px.scatter_mapbox(
        lat=[latest["latitude"]],
        lon=[latest["longitude"]],
        hover_name=[f"Taxi: {taxi_id}"],
        zoom=12,
        height=600
    )
    fig.update_layout(mapbox_style="open-street-map")

    # Add route if available
    if route:
        route_lat = [p["latitude"] for p in route]
        route_lon = [p["longitude"] for p in route]
        fig.add_scattermapbox(
            lat=route_lat,
            lon=route_lon,
            mode="lines",
            line=dict(width=2, color='blue'),
            name="Route"
        )

    return fig

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
