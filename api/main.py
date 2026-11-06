import asyncio
import signal
from pathlib import Path
from fastapi import FastAPI, APIRouter, HTTPException

API_VERSION = "v1"

app = FastAPI()
router = APIRouter(prefix=f"/{API_VERSION}")
process = None
process_lock = asyncio.Lock()
stop_requested = False
last_pid = None
last_returncode = None


@router.get("/status")
async def status():
    async with process_lock:
        if process is None:
            state = "IDLE" if last_pid is None else "STOPPED"
            return {"state": state, "pid": last_pid, "returncode": last_returncode}
        if process.returncode is not None:
            state = "STOPPED" if process.returncode == 0 else "FAILED"
        else:
            state = "STOPPING" if stop_requested else "RUNNING"
        return {"state": state, "pid": process.pid, "returncode": process.returncode}


@router.post("/run")
async def run(scenario: str):
    global process, stop_requested
    async with process_lock:
        if process is None or process.returncode is not None:
            config_path = Path("/home/sumo/assets") / scenario / f"{scenario}.sumocfg"
            if not config_path.is_file():
                raise HTTPException(status_code=404, detail="SUMO scenario configuration not found")
            process = await asyncio.create_subprocess_exec("sumo-gui", "--remote-port", "3000", "-S", "--quit-on-end", "-c", str(config_path))
            stop_requested = False
            return {"message": f"Start process {process}"}
        else:
            return {"message": f"Process {process} already in progress"}


@router.post("/stop")
async def stop():
    global process, stop_requested, last_pid, last_returncode
    async with process_lock:
        if process is not None:
            if process.returncode is None and not stop_requested:
                try:
                    process.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    pass
                stop_requested = True
            try:
                await asyncio.wait_for(process.wait(), timeout=30)
            except asyncio.TimeoutError:
                raise HTTPException(status_code=504, detail="SUMO did not stop within 30 seconds")
            last_pid = process.pid
            last_returncode = process.returncode
            process = None
            stop_requested = False
        return {"state": "IDLE" if last_pid is None else "STOPPED", "pid": last_pid, "returncode": last_returncode}


app.include_router(router)
