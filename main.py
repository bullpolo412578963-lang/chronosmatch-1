"""
ChronosMatch - Zero-Copy High-Frequency Trading Simulation Engine
Main CLI entrypoint. Supports live interactive dashboard, benchmark execution,
automated test execution, and CSV trade export.
"""

import sys
import os
import time
import argparse
import asyncio
import threading
import signal
from pathlib import Path

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.utils.config import (
    DEFAULT_CONFIG,
    SystemConfig,
    ticks_to_dollars,
)
from src.ipc.ring_buffer import SharedRingBuffer
from src.engine import create_engine, CYTHON_AVAILABLE
from src.market.simulator import AsyncMarketSimulator
from src.monitoring.latency import LatencyTracker, explain_clock_resolution
from src.monitoring.dashboard import TerminalDashboard
from src.storage.database import TradeDatabase, AsyncTradePersister
from src.storage.exporter import export_trades_to_csv


def run_simulation(
    engine_type: str = "cython",
    total_orders: int = 10000,
    rate_per_sec: int = 25000,
    headless: bool = False,
    shm_path: str = DEFAULT_CONFIG.ipc.file_path,
    capacity: int = 65536,
) -> None:
    """
    Execute full end-to-end trading simulation:
    Simulator -> Shared Memory Ring Buffer -> Matching Engine -> Latency Tracker -> SQLite
    """
    print(f"\n[ChronosMatch] Initializing simulation: Engine={engine_type.upper()}, Orders={total_orders:,}, Rate={rate_per_sec:,}/s, Headless={headless}")

    # 1. Initialize Shared Memory Ring Buffer
    ring_buffer = SharedRingBuffer(
        name=shm_path,
        capacity=capacity,
        create=True,
        file_backed=True,
    )

    # 2. Initialize Matching Engine
    engine = create_engine(engine_type=engine_type)

    # 3. Initialize Latency Tracker
    latency_tracker = LatencyTracker(sample_capacity=min(total_orders + 1000, 500000))

    # 4. Initialize Database and Async Persister
    trade_db = TradeDatabase()
    persister = AsyncTradePersister(db=trade_db)
    persister.start()

    # 5. Initialize Market Simulator
    simulator = AsyncMarketSimulator(ring_buffer=ring_buffer)

    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        print("\n[ChronosMatch] Interrupted by user. Initiating graceful shutdown...")
        shutdown_event.set()
        simulator.stop()
        ring_buffer.signal_shutdown()

    signal.signal(signal.SIGINT, signal_handler)

    # Consumer thread running the matching engine hot loop
    def matching_consumer():
        processed = 0
        while not shutdown_event.is_set():
            # Read directly from ring buffer
            raw = ring_buffer.read_order_raw(block=True, timeout=0.01)
            if raw is None:
                if ring_buffer.is_producer_done() and ring_buffer.is_empty():
                    break
                continue

            dequeue_ts = time.perf_counter_ns()
            order_id, seq, ts_ns, price, qty, side, order_type, reserved, client_id, checksum = raw

            match_start = time.perf_counter_ns()
            trades = engine.process_order_raw(
                order_id=order_id,
                seq=seq,
                timestamp_ns=ts_ns,
                price=price,
                qty=qty,
                side=side,
                order_type=order_type,
                client_id=client_id,
            )
            match_end = time.perf_counter_ns()

            # Record latency
            latency_tracker.record_stage(ts_ns, dequeue_ts, match_start, match_end)

            # Enqueue trades for async persistence
            if trades:
                persister.record_trades_batch(trades)

            processed += 1
            if total_orders > 0 and processed >= total_orders:
                break

    consumer_thread = threading.Thread(target=matching_consumer, name="MatchingConsumer", daemon=True)
    consumer_thread.start()

    # Producer thread running async simulator
    def producer_worker():
        if total_orders > 0:
            asyncio.run(simulator.run_benchmark_volume(total_orders))
        else:
            asyncio.run(simulator.run_continuous(target_rate_per_sec=rate_per_sec))

    producer_thread = threading.Thread(target=producer_worker, name="MarketProducer", daemon=True)
    producer_thread.start()

    start_sim_time = time.perf_counter()

    # UI / Monitoring
    if not headless:
        from rich.live import Live
        dashboard = TerminalDashboard()
        layout = dashboard.create_layout()

        with Live(layout, refresh_per_second=10, screen=True) as live:
            while consumer_thread.is_alive() and not shutdown_event.is_set():
                depth = engine.get_depth(5)
                sim_stats = simulator.stats
                lat_summary = latency_tracker.get_summary()

                elapsed = max(0.001, time.perf_counter() - start_sim_time)
                ops = engine.total_orders_processed / elapsed

                dashboard.update_layout(
                    layout=layout,
                    engine_type=engine_type,
                    depth=depth,
                    total_orders=engine.total_orders_processed,
                    buy_orders=sim_stats.total_buy,
                    sell_orders=sim_stats.total_sell,
                    matched_trades=engine.total_trades_executed,
                    pending_orders=engine.pending_orders_count,
                    orders_per_sec=ops,
                    latency_summary=lat_summary,
                    buffer_capacity=ring_buffer.capacity,
                    occupied_slots=ring_buffer.occupied,
                    utilization_pct=ring_buffer.utilization_pct,
                    dropped_count=ring_buffer.get_dropped_count(),
                    status="RUNNING",
                )
                time.sleep(0.08)

            # Final status update
            depth = engine.get_depth(5)
            lat_summary = latency_tracker.get_summary()
            dashboard.update_layout(
                layout=layout,
                engine_type=engine_type,
                depth=depth,
                total_orders=engine.total_orders_processed,
                buy_orders=simulator.stats.total_buy,
                sell_orders=simulator.stats.total_sell,
                matched_trades=engine.total_trades_executed,
                pending_orders=engine.pending_orders_count,
                orders_per_sec=engine.total_orders_processed / max(0.001, time.perf_counter() - start_sim_time),
                latency_summary=lat_summary,
                buffer_capacity=ring_buffer.capacity,
                occupied_slots=ring_buffer.occupied,
                utilization_pct=ring_buffer.utilization_pct,
                dropped_count=ring_buffer.get_dropped_count(),
                status="COMPLETED",
            )
            time.sleep(0.5)

    # Wait for consumer to finish processing all orders
    consumer_thread.join(timeout=10.0)
    total_elapsed = time.perf_counter() - start_sim_time

    # Drain and stop persister
    persister.stop(timeout=5.0)

    # Audit log
    lat_summary = latency_tracker.get_summary()
    e2e_stats = lat_summary.get("e2e")
    mean_lat = e2e_stats.mean_us if e2e_stats else 0.0
    p99_lat = e2e_stats.p99_us if e2e_stats else 0.0
    ops = engine.total_orders_processed / max(0.0001, total_elapsed)

    session_id = f"sim_{int(time.time())}"
    persister.record_session_audit(
        session_id=session_id,
        engine_type=engine_type,
        total_orders=engine.total_orders_processed,
        total_trades=engine.total_trades_executed,
        total_volume=engine.total_volume_traded,
        elapsed_sec=total_elapsed,
        orders_per_sec=ops,
        mean_latency_us=mean_lat,
        p99_latency_us=p99_lat,
    )

    # Cleanup ring buffer
    ring_buffer.unlink()

    # Print summary
    print("\n" + "=" * 70)
    print("CHRONOSMATCH SIMULATION SUMMARY")
    print("=" * 70)
    print(f"Engine Type:          {engine_type.upper()}")
    print(f"Orders Processed:     {engine.total_orders_processed:,}")
    print(f"Trades Executed:      {engine.total_trades_executed:,}")
    print(f"Shares Traded:        {engine.total_volume_traded:,}")
    print(f"Trades Persisted:     {persister.total_persisted:,}")
    print(f"Elapsed Time:         {total_elapsed:.3f} s")
    print(f"Overall Throughput:   {ops:,.0f} orders/sec")
    if e2e_stats and e2e_stats.count > 0:
        print(f"End-to-End Latency:   Mean: {e2e_stats.mean_us:.2f} μs | P50: {e2e_stats.median_us:.2f} μs | P99: {e2e_stats.p99_us:.2f} μs | Max: {e2e_stats.max_us:.2f} μs")
    print(f"Database Integrity:   {'PASSED' if trade_db.verify_integrity() else 'FAILED'}")
    print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="ChronosMatch: Zero-Copy High-Frequency Trading Simulation Engine"
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=10000,
        help="Number of orders to simulate (default: 10,000; 0 for continuous)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=50000,
        help="Target order rate per second (default: 50,000; 0 for unconstrained)",
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["cython", "python"],
        default="cython",
        help="Matching engine implementation (default: cython)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run without terminal dashboard for maximum throughput",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run full performance benchmarking suite",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Execute automated test suite via pytest",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export SQLite trade records to CSV",
    )
    parser.add_argument(
        "--clock-info",
        action="store_true",
        help="Display technical documentation on timer resolution and measurement accuracy",
    )

    args = parser.parse_args()

    if args.clock_info:
        print(explain_clock_resolution())
        return

    if args.test:
        import pytest
        sys.exit(pytest.main(["tests", "-v"]))

    if args.export:
        print("\n[ChronosMatch] Exporting trade records from SQLite to CSV...")
        summary = export_trades_to_csv()
        print(f"Export complete: {summary['output_file']}")
        print(f"Total Trades:    {summary['total_trades']:,}")
        print(f"Total Shares:    {summary['total_shares']:,}")
        print(f"Notional Value:  ${summary['total_notional_value']:,.2f}")
        return

    if args.benchmark:
        from benchmarks.benchmark_report import run_all_benchmarks_and_report
        run_all_benchmarks_and_report()
        return

    # Default: Run simulation
    run_simulation(
        engine_type=args.engine,
        total_orders=args.orders,
        rate_per_sec=args.rate,
        headless=args.headless,
    )


if __name__ == "__main__":
    main()
