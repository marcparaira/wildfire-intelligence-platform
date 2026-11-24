import { useEffect, useMemo, useState } from "react";
import MapView from "./MapView";
import type { EnrichmentLayer } from "./enrichmentLayers";
import { EnrichmentLayers } from "./EnrichmentLayers";
import { reportIncidentId, useIncidentDetail, useIncidents, type IncidentStatus } from "./incidents";
import { IncidentDetails } from "./IncidentPanel";
import { IncidentReport } from "./IncidentReport";
import { incidentStatuses } from "./incidents";
import type { IncidentSummary } from "./incidents";
import { FRP_HIGH_COLOR, FRP_LOW_COLOR, FRP_UNAVAILABLE_COLOR, frpPosition } from "./frp";
import { DEFAULT_VIEW_BOUNDS, formatUtc, inDateRange, isRegionId, mergeHotspots, regions, todayUtc } from "./hotspots";
import type { Bounds, DateRange, Hotspot, HotspotResults, RegionId } from "./hotspots";
import { useLiveDetections } from "./live";
import { usePipelineStatus } from "./pipeline";
import { PipelineStatusPage } from "./PipelineStatus";
import type { LiveStatus } from "./live";
import { formatWeatherTime, isWeatherMetric, weatherLegend, weatherMetrics } from "./weather";
import type { WeatherForecast, WeatherMetric } from "./weather";

interface Filters extends DateRange {
  region: RegionId | "";
}

function initialFilters(): Filters {
  const now = new Date();
  const from = new Date(now.getTime() - 6 * 24 * 60 * 60 * 1000);
  return {
    region: "",
    dateFrom: from.toISOString().slice(0, 10),
    dateTo: now.toISOString().slice(0, 10),
    openEnded: true,
  };
}

function regionQuery(region: RegionId | ""): URLSearchParams {
  const params = new URLSearchParams();
  if (region) {
    const [west, south, east, north] = regions[region].bounds;
    params.set("west", String(west));
    params.set("south", String(south));
    params.set("east", String(east));
    params.set("north", String(north));
  }
  return params;
}

type View = { kind: "map" } | { kind: "status" } | { kind: "report"; incidentId: string };

function currentView(): View {
  const hash = window.location.hash;
  if (hash === "#/status") return { kind: "status" };
  const incidentId = reportIncidentId(hash);
  return incidentId ? { kind: "report", incidentId } : { kind: "map" };
}

function useHashView(): View {
  const [view, setView] = useState(currentView);
  useEffect(() => {
    const update = () => setView(currentView());
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  return view;
}

const liveLabels: Record<LiveStatus, string> = {
  connecting: "Connecting…",
  live: "Live",
  reconnecting: "Reconnecting…",
  unavailable: "Live updates unavailable",
};

function App() {
  const [draft, setDraft] = useState(initialFilters);
  const [filters, setFilters] = useState(draft);
  const [results, setResults] = useState<HotspotResults | null>(null);
  const [liveItems, setLiveItems] = useState<Hotspot[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedIncidentId, setSelectedIncidentId] = useState<string | null>(null);
  const [incidentFocus, setIncidentFocus] = useState<Bounds | null>(null);
  const [incidentStatusFilter, setIncidentStatusFilter] = useState<Set<IncidentStatus>>(new Set(["new", "growing", "stable"]));
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [formError, setFormError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [filtersOpen, setFiltersOpen] = useState(true);
  const [observationsOpen, setObservationsOpen] = useState(true);
  const [weatherMetric, setWeatherMetric] = useState<WeatherMetric>("");
  const [weatherForecast, setWeatherForecast] = useState<WeatherForecast | null>(null);
  const [weatherAt, setWeatherAt] = useState("");
  const [weatherLoading, setWeatherLoading] = useState(false);
  const [weatherError, setWeatherError] = useState("");
  const [enrichmentLayers, setEnrichmentLayers] = useState<Set<EnrichmentLayer>>(new Set());

  useEffect(() => {
    const controller = new AbortController();
    const params = regionQuery(filters.region);
    params.set("date_from", filters.dateFrom);
    params.set("date_to", filters.dateTo);
    setLoading(true);
    setError("");
    setResults(null);
    setSelectedId(null);
    fetch(`/api/hotspots?${params}`, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`Request failed (${response.status})`);
        return response.json() as Promise<HotspotResults>;
      })
      .then((data) => setResults(data))
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          setError(reason instanceof Error ? reason.message : "Unable to load hotspot data");
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [filters, refresh]);

  useEffect(() => {
    if (!weatherMetric) {
      setWeatherForecast(null);
      setWeatherError("");
      setWeatherLoading(false);
      return;
    }

    const controller = new AbortController();
    const params = regionQuery(filters.region);
    if (weatherAt) params.set("at", weatherAt);

    setWeatherLoading(true);
    setWeatherError("");
    fetch(`/api/weather/forecast?${params}`, { signal: controller.signal })
      .then(async (response) => {
        if (response.status === 422 && weatherAt) {
          setWeatherAt("");
          return null;
        }
        if (!response.ok) {
          const error = await response.json() as { detail?: string };
          throw new Error(error.detail ?? `Request failed (${response.status})`);
        }
        return response.json() as Promise<WeatherForecast>;
      })
      .then((data) => {
        if (data) setWeatherForecast(data);
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          setWeatherError(reason instanceof Error ? reason.message : "Unable to load forecast");
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setWeatherLoading(false);
      });
    return () => controller.abort();
  }, [filters, weatherAt, weatherMetric, refresh]);

  useEffect(() => {
    setLiveItems([]);
    setSelectedIncidentId(null);
  }, [filters]);

  const view = useHashView();
  const pipeline = usePipelineStatus(view.kind === "status" ? 15_000 : 60_000);

  const liveQuery = useMemo(() => regionQuery(filters.region).toString(), [filters.region]);
  const incidents = useIncidents(liveQuery, refresh);
  const filteredIncidents = useMemo(
    () => incidents.items.filter((incident) => incidentStatusFilter.has(incident.status)),
    [incidents.items, incidentStatusFilter]
  );
  const selectedIncident = filteredIncidents.find((incident) => incident.incident_id === selectedIncidentId) ?? null;
  const incidentDetail = useIncidentDetail(selectedIncidentId, selectedIncident?.revision ?? null);
  const liveStatus = useLiveDetections(liveQuery, (batch) => {
    const current = batch.filter((hotspot) => inDateRange(hotspot, filters));
    if (current.length > 0) {
      setLiveItems((previous) => mergeHotspots(previous, current).items);
    }
  });

  const merged = useMemo(() => mergeHotspots(results?.items ?? [], liveItems), [results, liveItems]);
  const hotspots = results ? merged.items : [];
  const truncated = Boolean(results?.truncated) || merged.trimmed;
  const liveCount = hotspots.filter((hotspot) => hotspot.id === null).length;
  const areaBounds = filters.region ? regions[filters.region].bounds : null;
  const timelineValue = weatherAt || weatherForecast?.valid_at || "";
  const timelineIndex = Math.max(0, weatherForecast?.times.indexOf(timelineValue) ?? 0);
  const activeWeatherLegend = weatherLegend(weatherMetric);

  function selectHotspot(eventId: string) {
    setSelectedId(eventId);
    setSelectedIncidentId(null);
    setObservationsOpen(true);
  }

  function selectIncident(incidentId: string, focus: IncidentSummary | null = null) {
    setSelectedIncidentId(incidentId);
    setSelectedId(null);
    setObservationsOpen(true);
    if (focus) setIncidentFocus(focus.bbox);
  }

  function applyAreaFilter(region: RegionId | "") {
    setDraft({ ...draft, region });
    // Validate date range before applying
    if (draft.dateFrom && draft.dateTo && draft.dateFrom <= draft.dateTo) {
      setFormError("");
      setFilters({ ...draft, region, openEnded: draft.dateTo >= todayUtc() });
    } else if (draft.dateFrom && draft.dateTo) {
      setFormError("Choose a valid date range. Start must be on or before end.");
    }
  }

  function applyDateFilter(dateFrom: string, dateTo: string) {
    const newDraft = { ...draft, dateFrom, dateTo };
    setDraft(newDraft);
    // Validate and apply date range
    if (dateFrom && dateTo && dateFrom <= dateTo) {
      setFormError("");
      setFilters({ ...newDraft, openEnded: dateTo >= todayUtc() });
    } else if (dateFrom && dateTo) {
      setFormError("Choose a valid date range. Start must be on or before end.");
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand"><h1>Wildfire Intelligence Platform</h1></div>
        <nav className="topnav" aria-label="Views">
          <a href="#/" aria-current={view.kind === "map" ? "page" : undefined}>Map</a>
          <a href="#/status" aria-current={view.kind === "status" ? "page" : undefined}>Pipeline status</a>
        </nav>
        <span className="header-source">NASA FIRMS · VIIRS &amp; MODIS NRT · Open-Meteo</span>
      </header>

      {view.kind === "status" && <main className="status-main"><PipelineStatusPage state={pipeline} /></main>}
      {view.kind === "report" && <main className="status-main"><IncidentReport key={view.incidentId} incidentId={view.incidentId} /></main>}
      {view.kind === "map" && <main className="workspace">
        <aside className="sidebar" aria-label="Filters" data-collapsed={!filtersOpen}>
          <div className="panel-heading panel-heading-toggle">
            <h2 hidden={!filtersOpen}>Filters</h2>
            <button type="button" className="panel-toggle" aria-label={filtersOpen ? "Collapse filters" : "Expand filters"} title={filtersOpen ? "Collapse filters" : "Expand filters"} aria-expanded={filtersOpen} aria-controls="filter-panel" onClick={() => setFiltersOpen((open) => !open)}>
              <svg aria-hidden="true" viewBox="0 0 16 16"><polyline points={filtersOpen ? "10.5 3 5.5 8 10.5 13" : "5.5 3 10.5 8 5.5 13"} /></svg>
            </button>
          </div>
          <form id="filter-panel" className="filters" hidden={!filtersOpen}>
            <label htmlFor="region">Geographic area</label>
            <select id="region" value={draft.region} onChange={(event) => {
              const value = event.target.value;
              if (value === "" || isRegionId(value)) {
                applyAreaFilter(value);
              } else {
                setFormError("Choose a valid geographic area.");
              }
            }}>
              <option value="">No area filter</option>
              {Object.entries(regions).map(([key, region]) => <option key={key} value={key}>{region.label}</option>)}
            </select>
            <p className="field-note">Areas use approximate, overlapping bounding boxes, not administrative boundaries.</p>
            <div className="date-label">Observation dates <span>UTC</span></div>
            <div className="date-fields">
              <div><label htmlFor="date-from">From</label><input id="date-from" type="date" value={draft.dateFrom} onChange={(event) => applyDateFilter(event.target.value, draft.dateTo)} /></div>
              <div><label htmlFor="date-to">To</label><input id="date-to" type="date" value={draft.dateTo} onChange={(event) => applyDateFilter(draft.dateFrom, event.target.value)} /></div>
            </div>
            {formError && <p className="form-error" role="alert">{formError}</p>}
          </form>
          <div hidden={!filtersOpen}>
            <fieldset>
              <legend>Incident Status</legend>
              <div className="filter-buttons">
                {(["new", "growing", "stable"] as const).map((status) => (
                  <button
                    key={status}
                    className={`status-button ${incidentStatusFilter.has(status) ? "active" : ""}`}
                    onClick={() => {
                      const newFilter = new Set(incidentStatusFilter);
                      if (newFilter.has(status)) {
                        newFilter.delete(status);
                      } else {
                        newFilter.add(status);
                      }
                      setIncidentStatusFilter(newFilter);
                    }}
                    style={{
                      backgroundColor: incidentStatusFilter.has(status) ? incidentStatuses[status].color : "transparent",
                      color: incidentStatusFilter.has(status) ? "white" : "inherit",
                      border: `2px solid ${incidentStatuses[status].color}`,
                    }}
                  >
                    {incidentStatuses[status].label}
                  </button>
                ))}
              </div>
            </fieldset>
          </div>
          <div hidden={!filtersOpen}>
            <EnrichmentLayers shown={enrichmentLayers} onChange={setEnrichmentLayers} />
          </div>
        </aside>

        <section className="content" aria-label="Hotspot dashboard">
          <div className="map-toolbar">
            <div className="map-title"><h2>Hotspots</h2><span>{filters.region ? regions[filters.region].label : "All areas"}</span></div>
            <div className="toolbar-actions">
              <label className="weather-select">Weather layer
                <select aria-label="Weather layer" value={weatherMetric} onChange={(event) => {
                  if (isWeatherMetric(event.target.value)) setWeatherMetric(event.target.value);
                }}>
                  {weatherMetrics.map((metric) => <option key={metric.value} value={metric.value}>{metric.label}</option>)}
                </select>
              </label>
              <button className="refresh-button" type="button" onClick={() => setRefresh((value) => value + 1)}>Refresh data</button>
            </div>
          </div>
          <div className="map-summary">
            <span>Detections <strong>{loading || !results ? "—" : hotspots.length.toLocaleString()}</strong></span>
            <span>Latest <strong>{hotspots[0] ? formatUtc(hotspots[0].observed_at) : "—"}</strong></span>
            {liveCount > 0 && <span>New since load <strong>{liveCount.toLocaleString()}</strong></span>}
            {truncated && <span className="limit-label">Result limit reached</span>}
            <span className="live-status" data-status={liveStatus} role="status" title="New detections appear on the map as they arrive in the pipeline"><i aria-hidden="true" />{liveLabels[liveStatus]}</span>
          </div>
          {weatherMetric && weatherForecast && (
            <div className="weather-timeline">
              <label htmlFor="forecast-time">
                Forecast time <output>{formatWeatherTime(timelineValue)}</output>
              </label>
              <input
                id="forecast-time"
                type="range"
                min={0}
                max={Math.max(0, weatherForecast.times.length - 1)}
                value={timelineIndex}
                disabled={weatherForecast.times.length < 2}
                onChange={(event) => setWeatherAt(weatherForecast.times[Number(event.target.value)])}
              />
              <span>Forecast retrieved {formatWeatherTime(weatherForecast.generated_at)}</span>
            </div>
          )}
          {weatherLoading && <div className="map-message" role="status">Loading Open-Meteo forecast…</div>}
          {weatherError && <div className="map-message map-error" role="alert">Could not load weather forecast: {weatherError}.</div>}
          {loading && <div className="map-message" role="status">Loading observations…</div>}
          {error && <div className="map-message map-error" role="alert">Could not load observations: {error}. Check the API and try again.</div>}
          {!loading && !error && hotspots.length === 0 && <div className="map-message" role="status">No detections for these filters. Try a wider area or date range.</div>}
          {truncated && <div className="notice" role="status">Showing the most recent 5,000 detections. Narrow your filters to see the rest.</div>}
          <div className="map-wrap">
            <MapView
              hotspots={hotspots}
              weatherPoints={weatherForecast?.items ?? []}
              weatherMetric={weatherError ? "" : weatherMetric}
              bounds={areaBounds ?? DEFAULT_VIEW_BOUNDS}
              areaBounds={areaBounds}
              selectedId={selectedId}
              onSelect={selectHotspot}
              incidents={filteredIncidents}
              selectedIncidentId={selectedIncidentId}
              onSelectIncident={(id) => {
                const incident = filteredIncidents.find((inc) => inc.incident_id === id);
                selectIncident(id, incident ?? null);
              }}
              focusBounds={incidentFocus}
            />
          </div>
          <div className="map-footer">
            <div className="legend" role="group" aria-label="FRP legend, logarithmic scale, capped visually at 100 megawatts">
              <span>FRP (MW, log; ≥100 capped)</span>
              <div className="legend-scale">
                <div className="legend-ramp" aria-hidden="true">
                  <i className="legend-small" style={{ backgroundColor: FRP_LOW_COLOR }} />
                  <span style={{ background: `linear-gradient(to right, ${FRP_LOW_COLOR}, ${FRP_HIGH_COLOR})` }} />
                  <i className="legend-large" style={{ backgroundColor: FRP_HIGH_COLOR }} />
                </div>
                <div className="legend-ticks"><span>0</span><span style={{ left: `${frpPosition(10) * 100}%` }}>10</span><span>≥100</span></div>
              </div>
              <span className="legend-unavailable"><i style={{ backgroundColor: FRP_UNAVAILABLE_COLOR }} aria-hidden="true" /> Unavailable</span>
            </div>
            {activeWeatherLegend && <div className="weather-legend" role="group" aria-label={`${weatherMetrics.find((metric) => metric.value === weatherMetric)?.label} forecast legend`}>
              <span>{weatherMetrics.find((metric) => metric.value === weatherMetric)?.label}</span>
              <span className="weather-ramp" style={{ background: activeWeatherLegend.gradient }} />
              <span>{activeWeatherLegend.label}</span>
              {weatherMetric === "wind" && <span>Arrows show downwind direction</span>}
            </div>}
            {filteredIncidents.length > 0 && <div className="incident-legend" role="group" aria-label="Incident status legend">
              <span>Incidents</span>
              {(["new", "growing", "stable"] as const).map((status) => <span key={status}><i aria-hidden="true" style={{ borderColor: incidentStatuses[status].color }} />{incidentStatuses[status].label}</span>)}
            </div>}
            {areaBounds && <span>Dashed outline: area filter</span>}
            {weatherMetric && <span>Forecast by <a href="https://open-meteo.com/" target="_blank" rel="noreferrer">Open-Meteo</a>; forecasts do not indicate fire behavior.</span>}
            <span>FRP is radiated power, not fire severity or danger. Detections are not fire perimeters.</span>
          </div>
        </section>

        <aside className="detail-column" aria-label="Incident details" data-collapsed={!observationsOpen || !selectedIncidentId}>
          <div className="panel-heading panel-heading-toggle observation-heading" hidden={!selectedIncidentId}>
            <button type="button" className="panel-toggle" aria-label={observationsOpen ? "Collapse incident details" : "Expand incident details"} title={observationsOpen ? "Collapse incident details" : "Expand incident details"} aria-expanded={observationsOpen} aria-controls="observation-panel" onClick={() => setObservationsOpen((open) => !open)}>
              <svg aria-hidden="true" viewBox="0 0 16 16"><polyline points={observationsOpen ? "5.5 3 10.5 8 5.5 13" : "10.5 3 5.5 8 10.5 13"} /></svg>
            </button>
            <h2 hidden={!observationsOpen}>Incident details</h2>
          </div>
          <div id="observation-panel" hidden={!observationsOpen || !selectedIncidentId}>
            <section className="details">
              <IncidentDetails summary={selectedIncident} incident={incidentDetail.incident} loading={incidentDetail.loading} error={incidentDetail.error} />
            </section>
          </div>
        </aside>
      </main>}
    </div>
  );
}


export default App;
