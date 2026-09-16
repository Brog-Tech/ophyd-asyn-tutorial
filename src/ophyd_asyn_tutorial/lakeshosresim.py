"""Used for tutorial `Using Devices`."""

# Import bluesky and ophyd
import asyncio
from dataclasses import dataclass
from functools import cached_property
from math import isclose
from typing import Annotated as A
from typing import ClassVar
from bisect import bisect
from time import perf_counter


import bluesky.plan_stubs as bps
import bluesky.plans as bp
# import bluesky.preprocessors as bpp
from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.run_engine import RunEngine
from bluesky.utils import ProgressBarManager
from IPython import get_ipython
from ophyd_async.core import (
    MovableLogic,
    SignalR,
    SignalRW,
    StandardMovable,
    StandardReadable,
    TimeoutCalculator,
    init_devices,
)
from ophyd_async.core import StandardReadableFormat as Format
from ophyd_async.epics.core import EpicsDevice, PvSuffix

ipy = get_ipython()

# Use this ipythhon config for autocompletion of class attributes
if ipy is not None:
    ipy.Completer.use_jedi = True

# Manual version
# %config IPCompleter.use_jedi = True

# Create a run engine and make ipython use it for `await` commands
RE = RunEngine(call_returns_result=True)
RE.waiting_hook = ProgressBarManager()
# autoawait_in_bluesky_event_loop()

# Add a callback for plotting
bec = BestEffortCallback()
RE.subscribe(bec)


class LakeShore336Input(EpicsDevice, StandardReadable):
    """One Lake Shore 336 temperature input."""

    temp: A[SignalR[float], PvSuffix("T-I"), Format.HINTED_SIGNAL]
    tempc: A[SignalR[float], PvSuffix("T:C-I")]
    status: A[SignalR[str], PvSuffix("T-Sts")]


@dataclass
class LakeShore336LoopLogic(MovableLogic[float]):

    # def __init__(self)
    #     super().__init__(name)

    """Define what a move means for one Lake Shore control loop."""

    tolerance: float
    settle_time: float
    move_timeout: float
    p: SignalRW[float]
    i: SignalRW[float]
    d: SignalRW[float]
    ramp: SignalRW[float]

    delta = None

    async def check_move(self, new_position):
        if new_position < 5:
            raise ValueError
        elif new_position > 550:
            raise ValueError

    async def delta_t(
        self,
        old_position: float,
        new_position: float,
    ) -> None:
        self.delta = new_position - old_position

    async def set_pids(
        self,
        new_position: float
    ) -> None:

        if self.delta > .5:
            temp_thresholds = [140,270,300]
            pids = [(50,10,1),(25,6,3),(26,5,3),(26,5,3)]

        elif self.delta < -.5:
            temp_thresholds = [140,200]
            pids = [(25,4,3),(25,6,3),(35,8,3)]

        else:
            return

        pid_selector = bisect(temp_thresholds,new_position)
        _p,_i,_d = pids[pid_selector]

        await self.p.set(_p)
        await self.i.set(_i)
        await self.d.set(_d)        

    async def calculate_timeout(
        self,
        old_position: float,
        new_position: float,
    ) -> float:

        await self.delta_t(old_position,new_position)
        print(f"Delta = {self.delta}")

        await self.set_pids(new_position)

        ramp_rate = await self.ramp.get_value()  # perhaps degrees/minute

        if ramp_rate == 0:
            ramp_rate = .015 # K/s

        travel_time = abs(self.delta) / ramp_rate * 60
        print(f"{travel_time + self.settle_time + 30} Seconds to complete the temp change")
        return travel_time + self.settle_time + 30

    async def move(
        self, new_position: float, timeout: TimeoutCalculator
        ) -> None:
        """Write the setpoint and wait for readback to settle around it."""

        start_time = perf_counter

        original_temp = await self.readback.get_value()
        new_temp = await self.setpoint.get_value()

        print(f"Changing temp from {original_temp} to {new_temp}")

        loop = asyncio.get_running_loop()
        settled = asyncio.Event()
        settle_timer: asyncio.TimerHandle | None = None
        
        def update_settled_state(reading) -> None:
            nonlocal settle_timer
            value = reading[self.readback.name]["value"]
            in_tolerance = isclose(
                value, new_position, rel_tol=0.0, abs_tol=self.tolerance
            )

            if in_tolerance and settle_timer is None:
                # Start a timer the first time the readback enters tolerance.
                # It is cancelled below if a later update leaves tolerance.
                settle_timer = loop.call_later(self.settle_time, settled.set)
            elif not in_tolerance and settle_timer is not None:
                settle_timer.cancel()
                settle_timer = None
                settled.clear()

        # Subscribe before writing so that a fast readback update cannot be missed.
        self.readback.subscribe_reading(update_settled_state)
        try:
            # await self.setpoint.set(new_position, timeout=timeout())
            await self.setpoint.set(new_position)
            async with asyncio.timeout(timeout()):
                await settled.wait()
        finally:
            if settle_timer is not None:
                settle_timer.cancel()
            self.readback.clear_sub(update_settled_state)


class LakeShore336Loop(EpicsDevice, StandardReadable, StandardMovable[float]):
    """
    One heater/control loop.

    setpoint, PID, heater range, ramp settings, etc.
    """

    enbl: A[
        SignalRW[str], PvSuffix("Enbl-Sel")
    ]  # this is the general on and off for the loop

    # The setpoint is written by StandardMovable. The process readback is the
    # loop's primary reading and is used to decide when a move has completed.
    sp: A[SignalRW[float], PvSuffix("T-SP"), Format.CONFIG_SIGNAL]
    readback: A[SignalR[float], PvSuffix("T-RB"), Format.HINTED_SIGNAL]
    p: A[
        SignalRW[float],
        PvSuffix(write_suffix="Gain:P-SP", read_suffix="Gain:P-RB"),
        Format.CONFIG_SIGNAL,
    ]
    i: A[
        SignalRW[float],
        PvSuffix(write_suffix="Gain:I-SP", read_suffix="Gain:I-RB"),
        Format.CONFIG_SIGNAL,
    ]
    d: A[
        SignalRW[float],
        PvSuffix(write_suffix="Gain:D-SP", read_suffix="Gain:D-RB"),
        Format.CONFIG_SIGNAL,
    ]
    ramp: A[
        SignalRW[float],
        PvSuffix(write_suffix="Val:Ramp-SP", read_suffix="Val:Ramp-RB"),
        Format.CONFIG_SIGNAL,
    ]

    range: A[
        SignalRW[str],
        PvSuffix(write_suffix="Val:Range-Sel", read_suffix="Val:Range-Sts"),
        Format.CONFIG_SIGNAL,
    ]

    maxi: A[SignalRW[float], PvSuffix("Out:MaxI-SP")]
    loop_mode: A[SignalRW[str], PvSuffix("Mode-Sel"), Format.CONFIG_SIGNAL]
    resistance: A[SignalRW[str], PvSuffix("Out:R-SP"), Format.CONFIG_SIGNAL]
    ramp_enbl: A[SignalRW[str], PvSuffix("Enbl:Ramp-Sel"), Format.CONFIG_SIGNAL]
    autotune: A[
        SignalRW[str],
        PvSuffix(write_suffix="Mode:ATune-Sel", read_suffix="Mode:ATune-Sts"),
    ]

    def __init__(
        self,
        prefix: str,
        *,
        tolerance: float = 0.1,
        settle_time: float = 5.0,
        move_timeout: float = 600.0,
        name: str = "",
    ) -> None:
        self._tolerance = tolerance
        self._settle_time = settle_time
        self._move_timeout = move_timeout
        super().__init__(prefix, name=name)

    @cached_property
    def movable_logic(self) -> MovableLogic[float]:
        """Connect StandardMovable to this loop's setpoint and readback."""
        return LakeShore336LoopLogic(
            setpoint=self.sp,
            readback=self.readback,
            tolerance=self._tolerance,
            settle_time=self._settle_time,
            move_timeout=self._move_timeout,
            p=self.p,
            i=self.i,
            d=self.d,
            ramp = self.ramp
        )

    Out_Sel: A[SignalRW[float], PvSuffix("Out-Sel"), Format.CONFIG_SIGNAL]
    # Max: A[SignalRW[float], PvSuffix("Out:Max-SP"), Format.CONFIG_SIGNAL]
    # Disp: A[SignalRW[float], PvSuffix("Out:Disp-SP"), Format.CONFIG_SIGNAL]

    # def __init__(self, prefix, with_pvi = False, name = ""):
    #     super().__init__(prefix, with_pvi, name)
    #     config:str


class LakeShore336(StandardReadable):
    """Master Lake Shore 336 object."""

    def __init__(
        self,
        prefix: str,
        *,
        name: str = "",
    ):

        with self.add_children_as_readables():
            self.input_a = LakeShore336Input(f"{prefix}-Chan:A}}")
            self.input_b = LakeShore336Input(f"{prefix}-Chan:B}}")
            self.input_c = LakeShore336Input(f"{prefix}-Chan:C}}")
            self.input_d = LakeShore336Input(f"{prefix}-Chan:D}}")
            self.input_d2 = LakeShore336Input(f"{prefix}-Chan:D2}}")
            self.input_d3 = LakeShore336Input(f"{prefix}-Chan:D3}}")
            self.input_d4 = LakeShore336Input(f"{prefix}-Chan:D4}}")
            self.input_d5 = LakeShore336Input(f"{prefix}-Chan:D5}}")

            self.out1 = LakeShore336Loop(f"{prefix}-Out:1}}")
            self.out2 = LakeShore336Loop(f"{prefix}-Out:2}}")
            self.out3 = LakeShore336Loop(f"{prefix}-Out:3}}")
            self.out4 = LakeShore336Loop(f"{prefix}-Out:4}}")

        super().__init__(name=name)


with init_devices(mock=True):
    lake = LakeShore336("XF:28ID1-ES{LS336:1", name="test")


# exit()

"""
Heres and example plan for moving the cryostat and ignoring a setpoint
not being reached adn timing out.
"""
import warnings

cryostat = lake.out1

def move_and_continue(device, target):
    try:
        yield from bps.mv(device, target)
    except TimeoutError:
        warnings.warn(
            f"{device.name} did not settle at {target}; continuing",
            RuntimeWarning,
        )

# Compare out to sp
# RE(bp.scan([lake.input_a.temp],lake.out1,5,400,100))