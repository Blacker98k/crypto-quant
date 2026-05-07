# Dashboard Paper Metrics Design

## Goal

Surface the daily paper-trading rollup in the existing dashboard so a running paper session can be scanned without opening CLI JSON.

## Approach

Reuse the `/api/paper_metrics` endpoint added in the previous slice. The static Vue page keeps one `paperMetrics` reactive object with zero defaults and refreshes it inside the existing `loadAll()` polling loop. This avoids a separate timer and keeps all operational dashboard data moving together.

The panel is intentionally compact: filled notional, fees, cash PnL, risk-event count, open position count, open notional, and touched symbols. These are the fastest paper-session health checks before richer charts exist.

## UX

Keep the dashboard utilitarian and dense. The panel sits directly below top status and above market cards, using small labels, tabular numbers, and the existing positive/negative color convention. Cards use an 8px radius to make the interface feel more like an operations console than a marketing page.

## Testing

Add a static page test that asserts the dashboard references `paperMetrics`, calls `/api/paper_metrics`, and renders the key API fields. Existing endpoint tests continue to cover the JSON payload itself.
