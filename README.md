# CAVISE SUMO

This repository contains the SUMO resources used by the CAVISE simulation
environment, including reusable maps, routes, traffic scenarios, and the
Docker configuration for running SUMO.

The assets are shared with other CAVISE components, such as OpenCDA and Artery,
to support standalone SUMO simulations, CARLA–SUMO co-simulation, and connected
vehicle scenarios that combine road traffic simulation with Artery's V2X
network simulation.

## Repository structure

- `assets/` — SUMO networks, routes, scenario configurations, and related map
  files.
- `Dockerfile` — container environment with SUMO and its Python interfaces.
