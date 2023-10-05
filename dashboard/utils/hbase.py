import happybase

import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from .config import HBASE_URL

brands = [
    "samsung",
    "apple",
    "asus",
    "msi",
    "gigabyte",
    "dell",
    "hp",
    "lenovo",
    "sony",
    "intel",
]


class HBaseReader:
    def __init__(self) -> None:
        self.connection = happybase.Connection(HBASE_URL)
        # self.connection = happybase.Connection("localhost")

    def read(self):
        try:
            table = self.connection.table("electronic-analytics")
            data = {
                "brand": [],
                "view_count": [],
                "cart_count": [],
                "purchase_count": [],
            }
            for brand in brands:
                row = table.row(brand.encode(), columns=["report"])
                data["brand"].append(brand)
                data["view_count"].append(int(row[b"report:view_count"]))
                data["cart_count"].append(int(row[b"report:cart_count"]))
                data["purchase_count"].append(int(row[b"report:purchase_count"]))
            print(data)
            return pd.DataFrame.from_dict(data)
        except Exception as e:
            print(e)
            return None

    def fig(self):
        df = self.read()
        return px.histogram(
            df,
            x="brand",
            y=[
                "view_count",
                "cart_count",
                "purchase_count",
            ],
            barmode="group",
            title="Events by Brand",
        )
