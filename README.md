# Distributed Mutual Exclusion Simulator

A Flask web app that simulates four mutual exclusion algorithms (Centralized, Lamport, Ricart-Agrawala, Token Ring) and compares them.

## Run locally
    pip install -r requirements.txt
    python p2.py

Then open http://127.0.0.1:5055

## Deploy (Render)
- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn p2:app`
