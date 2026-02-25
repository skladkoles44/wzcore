# WZCore — System Documentation

WZCore is a deterministic message processing and dispatch pipeline implemented as Bot1 + Bot2 + SQLite.

This repository contains:
- Technical documentation (facts-only)
- Runtime snapshot of the production structure
- systemd unit configuration used in deployment

The repository is intentionally domain-agnostic.

## Components

Bot1  
- Ingests inbound events  
- Normalizes payloads  
- Persists events into SQLite  

SQLite  
- Durable storage layer  
- Events table  
- Delivery table  
- Explicit state transitions  

Bot2  
- Claims delivery tasks  
- Applies lease mechanism  
- Dispatches to delivery channels  
- Handles retries  

## Repository Layout

src/opt/max-bot1/  
Runtime snapshot of Bot1  

src/opt/max-bot2/  
Runtime snapshot of Bot2  

deploy_systemd/  
Production service definitions  

docs/  
System documentation and state machine specification  

## Versioning

VERSION — current version marker  
CHANGELOG.md — change history  
LICENSE — licensing information  

