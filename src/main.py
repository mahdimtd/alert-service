from fastapi import FastAPI

from src.api.routers import alerts, readings

app = FastAPI(title="Alert Service")

app.include_router(alerts.router)
app.include_router(readings.router)


@app.get("/health")
def health():
    return {"status": "ok"}