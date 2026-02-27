#!/usr/bin/env python3

import os

from diagrams import Diagram, Edge
from diagrams.custom import Custom

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ICON_DIR = os.path.join(BASE_DIR, "icons")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.chdir(OUTPUT_DIR)

scheduler_icon = os.path.join(ICON_DIR, "scheduler.png")
ingestion_icon = os.path.join(ICON_DIR, "ingestion.png")
database_icon = os.path.join(ICON_DIR, "database.png")
calculation_icon = os.path.join(ICON_DIR, "calculator.png")
registry_icon = os.path.join(ICON_DIR, "registry.png")
staging_icon = os.path.join(ICON_DIR, "staging.png")
reviewer_ui_icon = os.path.join(ICON_DIR, "display.png")

# sets general diagram settings for graphviz
graph_attr = {
    "margin": "-1",
    "nodesep": "1.1",
    "ranksep": "1.0",
    "splines": "polyline",
    "forcelabels": "true",
}

node_attr = {
    "fontname": "Helvetica-Bold",
    "fontsize": "12",
    "margin": "-.5"
}

edge_attr = {
    "penwidth": "2.0",
    "color": "darkblue",
    "fontsize": "10",
}

try:
    with Diagram(
        "",
        filename="grazeops_pipeline_architecture",
        outformat="png",
        show=False,
        direction="LR",
        graph_attr=graph_attr,
        node_attr=node_attr,
        edge_attr=edge_attr,
    ):
        scheduler = Custom("scheduler", scheduler_icon)
        ingestion = Custom("ingestion-worker", ingestion_icon)
        postgres_db = Custom("postgres-db", database_icon)
        calculation = Custom("calculation-service", calculation_icon)
        model_registry = Custom("model-registry", registry_icon)
        staging = Custom("staging-service", staging_icon)
        reviewer_ui = Custom("reviewer-ui", reviewer_ui_icon)

        scheduler >> Edge(taillabel="triggers", labeldistance="3", labelangle="28") >> ingestion
        ingestion >> Edge(taillabel="writes", labeldistance="3", labelangle="28") >> postgres_db
        postgres_db >> Edge(taillabel="reads", labeldistance="3", labelangle="24") >> calculation
        calculation >> Edge(taillabel="writes", labeldistance="3", labelangle="-24") >> postgres_db
        calculation >> Edge(taillabel="registers", labeldistance="3", labelangle="30") >> model_registry
        model_registry >> Edge(headlabel="candidate", labeldistance="3.0", labelangle="-32") >> staging
        staging >> Edge(headlabel="staging status", labeldistance="3.0", labelangle="32") >> model_registry
        reviewer_ui >> Edge(taillabel="inspects", labeldistance="3", labelangle="30") >> postgres_db
        reviewer_ui >> Edge(taillabel="inspects", labeldistance="3", labelangle="24") >> model_registry
except FileNotFoundError:
    pass
