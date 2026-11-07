import os
from api.persistence import Occupancy
from contextlib import asynccontextmanager
import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
import signal
from pathlib import Path
from fastapi import FastAPI, APIRouter, HTTPException
from fastapi import Request
from fastapi.responses import JSONResponse, Response

logger = logging.getLogger("sumo.api")

API_VERSION = "v1"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Stop and reap the managed simulator before the API exits."""
    global occupancy
    occupancy = Occupancy(Path(os.environ.get("CAVISE_ADAPTER_STATE_DIR", ".adapter-state")))
    try:
        yield
    finally:
        if process is not None:
            current = process
            try:
                await stop()
            except HTTPException:
                logger.warning("Forcing process shutdown: pid=%s", current.pid)
                try:
                    os.killpg(current.pid, signal.SIGKILL)
                except ProcessLookupError:
                    logger.info("Process already exited: pid=%s", current.pid)
                await current.wait()


occupancy: Occupancy | None = None


@asynccontextmanager
async def application_lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Close storage even if process cleanup fails, retaining durable uncertainty."""
    global occupancy
    try:
        async with lifespan(app):
            yield
    finally:
        if occupancy is not None:
            occupancy.close()
            occupancy = None


app = FastAPI(lifespan=application_lifespan)
router = APIRouter(prefix=f"/{API_VERSION}")
process = None
process_lock = asyncio.Lock()
stop_requested = False
last_pid = None
last_returncode = None


def _group_exists(group_id: int) -> bool:
    """Keep occupancy while the owned process group survives its launcher."""
    try:
        os.killpg(group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def _wait_for_group(current: asyncio.subprocess.Process) -> None:
    """Wait for the launcher and all owned children before declaring cleanup."""
    await current.wait()
    while _group_exists(current.pid):
        await asyncio.sleep(0.1)


@app.middleware("http")
async def log_requests(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
    """Report expected rejections and translate operating-system failures."""
    try:
        response = await call_next(request)
    except OSError:
        logger.exception("%s %s failed: pid=%s", request.method, request.url.path, process.pid if process else None)
        return JSONResponse(status_code=500, content={"detail": "SUMO operation failed"})
    if response.status_code >= 400:
        logger.warning("%s %s rejected: HTTP %s", request.method, request.url.path, response.status_code)
    elif request.url.path.endswith("/status"):
        logger.debug("Status requested: HTTP %s", response.status_code)
    return response


@router.get("/status")
async def status():
    if occupancy is not None and occupancy.uncertain:
        return {"state": "UNKNOWN", "pid": None, "returncode": None}
    async with process_lock:
        if process is None:
            state = "IDLE" if last_pid is None else ("STOPPED" if last_returncode == 0 else "FAILED")
            return {"state": state, "pid": last_pid, "returncode": last_returncode}
        if process.returncode is not None and not _group_exists(process.pid):
            state = "STOPPED" if process.returncode == 0 else "FAILED"
        else:
            state = "STOPPING" if stop_requested else "RUNNING"
        return {"state": state, "pid": process.pid, "returncode": process.returncode}


@router.post("/run")
async def run(scenario: str):
    global process, stop_requested
    logger.info("Run requested: scenario=%s", scenario)
    async with process_lock:
        if occupancy is not None and occupancy.uncertain:
            raise HTTPException(409, "Previous execution ownership is unknown; verify container cleanup")
        if process is None or (process.returncode is not None and not _group_exists(process.pid)):
            if occupancy is not None:
                occupancy.clear()
            assets_root = Path("/home/sumo/assets").resolve()
            config_path = (assets_root / scenario / f"{Path(scenario).name}.sumocfg").resolve()
            if not config_path.is_relative_to(assets_root):
                raise HTTPException(status_code=404, detail="SUMO scenario configuration not found")
            if not config_path.is_file():
                raise HTTPException(status_code=404, detail="SUMO scenario configuration not found")
            if occupancy is not None:
                occupancy.reserve()
            try:
                process = await asyncio.create_subprocess_exec(
                    "sumo-gui", "--remote-port", "3000", "-S", "--quit-on-end", "-c", str(config_path), start_new_session=True
                )
            except OSError:
                if occupancy is not None:
                    occupancy.clear()
                raise
            if occupancy is not None:
                occupancy.started(process.pid)
            stop_requested = False
            logger.info("Process started: scenario=%s pid=%s", scenario, process.pid)
            return {"message": f"Start process {process}"}
        else:
            logger.info("Process already running: pid=%s", process.pid)
            return {"message": f"Process {process} already in progress"}


@router.post("/stop")
async def stop():
    global process, stop_requested, last_pid, last_returncode
    if occupancy is not None and occupancy.uncertain:
        raise HTTPException(409, "Cannot signal a process whose ownership is unknown")
    logger.info("Stop requested")
    async with process_lock:
        if process is not None:
            if not stop_requested:
                try:
                    os.killpg(process.pid, signal.SIGINT)
                except ProcessLookupError:
                    logger.info("Process already exited before SIGINT: pid=%s", process.pid)
                stop_requested = True
            current = process
        else:
            current = None
    if current is not None:
        try:
            await asyncio.wait_for(_wait_for_group(current), timeout=30)
        except asyncio.TimeoutError:
            logger.error("Stop timed out: pid=%s", current.pid)
            raise HTTPException(status_code=504, detail="SUMO did not stop within 30 seconds")
        async with process_lock:
            if process is current:
                logger.info("Process stopped: pid=%s returncode=%s", current.pid, current.returncode)
                last_pid = current.pid
                last_returncode = current.returncode
                if occupancy is not None:
                    occupancy.clear()
                process = None
                stop_requested = False
    return await status()


app.include_router(router)
