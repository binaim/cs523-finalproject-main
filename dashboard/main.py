import threading
from confluent_kafka import Consumer
from dash import Dash, html, dcc, Output, Input

from utils.event import Event
from utils.hbase import HBaseReader
from utils.listener import EventListener
import plotly.graph_objs as go
from utils.config import KAFKA_URL, KAFKA_GROUP_ID, THREAD_DAEMON

# df = pd.read_csv("plotly/data.csv")
import flask

server = flask.Flask(__name__)

app = Dash(__name__, server=server)

# events = pd.DataFrame(columns=["date", "view", "purchase", "cart"])
event = Event()
hbase = HBaseReader()
listener = EventListener(on_new_event=lambda data: event.append(data))
# app.layout = html.Div(
#     [
#         html.H1(children="Title of Dash App", style={"textAlign": "center"}),
#         # dcc.Dropdown(df.country.unique(), "Canada", id="dropdown-selection"),
#         dcc.Graph(id="graph-content"),
#         dcc.Interval(
#             id="interval-component", interval=1 * 1000, n_intervals=0  # in milliseconds
#         ),
#     ]
# )
app.layout = html.Div(
    className="row",
    children=[
        html.H1(children="DashBoard", style={"textAlign": "center"}),
        html.Div(
            className="six columns",
            children=[
                dcc.Graph(figure=HBaseReader().fig(), id="brand-sales"),
            ],
        ),
        html.Div(
            className="six columns",
            children=[
                # dcc.Dropdown(df.country.unique(), "Canada", id="dropdown-selection"),
                dcc.Graph(id="graph-content"),
            ],
        ),
        dcc.Interval(
            id="interval-component",
            interval=1 * 1000,
            n_intervals=0,  # in milliseconds
        ),
    ],
)


@app.callback(
    Output("graph-content", "figure"), Input("interval-component", "n_intervals")
)
def update_graph_live(n):
    # print("Heartbeat")
    listener.consume()
    return event.fig()


# @callback(
#     Output(component_id="brand-sales", component_property="figure"),
#     Input("interval-component", "n_intervals"),
# )
# def update_histogram_graph(n):
#     df = hbase.read()
#     return px.histogram(df, x="brand", y="sales")


def kafka_listener():
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_URL,
            # "bootstrap.servers": "localhost:9092",
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe(["electronic-analytics"])
    while True:
        # print("Running...")
        msg = c.poll(1.0)
        if msg is None:
            # print("None Message")
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        key = msg.key().decode("utf-8") if msg.key() is not None else ""
        value = msg.value().decode("utf-8")

        print("Received message: {} ;; {}".format(key, value))

        if key == "event_type_agg":
            listener.add_value(value)


thread = threading.Thread(name="kafka consumer", target=kafka_listener)
thread.setDaemon(THREAD_DAEMON)
thread.start()

if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=True, host="0.0.0.0", port=8051)
