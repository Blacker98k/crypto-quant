# Simulation Degradation Gates Design

## Goal

Make long-running simulated paper checks fail loudly when they pass trading-flow assertions but degrade away from real exchange market prices.

## Shape

- Keep `simulate_paper.py` unchanged as the runner that records per-cycle payloads.
- Reuse simulation summaries' `price_sources` field so source drift is visible over time.
- Extend `scripts/summarize_simulation_report.py` with quality gates for:
  - minimum cycle count,
  - maximum failed cycles,
  - forbidden price-source prefixes such as `static_fallback`,
  - requiring every cycle to use one specific price source.
- Preserve the existing JSON summary output so reports remain machine-readable.

## Safety

The gates inspect local JSONL reports only. They do not place orders, use private credentials, or change live trading configuration.
