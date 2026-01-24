from typing import Any, Dict, List


class OpsDashboard:
    """Aggregate deterministic operational view from core managers.

    Expected inputs (optional):
    - incident_manager: IncidentManager
    - broker_monitor: BrokerHealthMonitor
    - data_manager: DataFeedManager
    - safe_mode: SafeModeManager
    - recovery_manager: RecoveryManager
    - postmortem_manager: PostmortemManager
    - market_crisis: MarketCrisisManager
    """

    def __init__(
        self,
        *,
        incident_manager=None,
        broker_monitor=None,
        data_manager=None,
        safe_mode=None,
        recovery_manager=None,
        postmortem_manager=None,
        market_crisis=None,
    ):
        self.incident_manager = incident_manager
        self.broker_monitor = broker_monitor
        self.data_manager = data_manager
        self.safe_mode = safe_mode
        self.recovery_manager = recovery_manager
        self.postmortem_manager = postmortem_manager
        self.market_crisis = market_crisis

    def system_health(self) -> Dict[str, Any]:
        """Return a deterministic health summary.

        - `ok`: bool overall
        - `details`: list of named health checks
        """
        checks: List[Dict[str, Any]] = []
        ok = True

        # brokers
        if self.broker_monitor is not None:
            failed = sorted(self.broker_monitor.failed_brokers())
            checks.append({"name": "brokers_failed", "failed": failed})
            if failed:
                ok = False

        # data feeds
        if self.data_manager is not None:
            # collect instruments with no fresh feed
            instruments = sorted(self.data_manager.hierarchy.keys())
            no_feed = [
                i
                for i in instruments
                if self.data_manager.best_available_feed(i) is None
            ]
            degraded = [i for i in instruments if self.data_manager.is_degraded(i)]
            checks.append({"name": "no_fresh_feed", "instruments": no_feed})
            checks.append({"name": "degraded_feeds", "instruments": sorted(degraded)})
            if no_feed:
                ok = False

        # safe mode
        if self.safe_mode is not None:
            checks.append(
                {"name": "global_halt", "flag": bool(self.safe_mode.global_halt)}
            )
            if self.safe_mode.global_halt:
                ok = False

        return {"ok": ok, "checks": checks}

    def active_incidents(self) -> List[Dict[str, Any]]:
        if self.incident_manager is None:
            return []
        incs = self.incident_manager.list_incidents()
        # deterministic ordering already ensured by list_incidents
        return [
            {"id": i.id, "title": i.title, "severity": i.severity.name, "ts": i.ts}
            for i in incs
        ]

    def broker_status(self) -> Dict[str, Any]:
        if self.broker_monitor is None:
            return {}
        brokers = sorted(self.broker_monitor.brokers.keys())
        status = {
            n: {
                "healthy": self.broker_monitor.is_healthy(n),
                "last_heartbeat": self.broker_monitor.brokers[
                    n
                ].last_heartbeat.isoformat(),
            }
            for n in brokers
        }
        failed = sorted(self.broker_monitor.failed_brokers())
        return {"brokers": status, "failed": failed}

    def feed_status(self) -> Dict[str, Any]:
        if self.data_manager is None:
            return {}
        instruments = sorted(self.data_manager.hierarchy.keys())
        feeds = {}
        for inst in instruments:
            best = self.data_manager.best_available_feed(inst)
            feeds[inst] = {
                "best_feed": best,
                "degraded": self.data_manager.is_degraded(inst),
                "recovered": self.data_manager.recovered(inst),
            }
        return {"instruments": feeds}

    def trading_mode(self) -> str:
        # precedence: safe_mode.halt -> killed market crisis -> degraded/safe -> normal
        if self.safe_mode is not None and self.safe_mode.global_halt:
            return "halt"
        if (
            self.market_crisis is not None
            and self.market_crisis.killed
            and not self.market_crisis.override_actor
        ):
            return "halt"
        # data degradation -> safe
        if self.data_manager is not None:
            any_degraded = any(
                self.data_manager.is_degraded(i)
                for i in self.data_manager.hierarchy.keys()
            )
            if any_degraded:
                return "safe"
        # recovery blocks trading
        if self.recovery_manager is not None and self.recovery_manager.in_recovery:
            return "halt"
        return "normal"

    def recovery_progress(self) -> Dict[str, Any]:
        if self.recovery_manager is None:
            return {"in_recovery": False}
        # checkpoint progress: index of last checkpoint and count
        last_idx = len(self.recovery_manager.checkpoints) - 1
        return {
            "in_recovery": bool(self.recovery_manager.in_recovery),
            "checkpoints": len(self.recovery_manager.checkpoints),
            "last_checkpoint_index": (last_idx if last_idx >= 0 else None),
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "system_health": self.system_health(),
            "active_incidents": self.active_incidents(),
            "broker_status": self.broker_status(),
            "feed_status": self.feed_status(),
            "trading_mode": self.trading_mode(),
            "recovery_progress": self.recovery_progress(),
        }
