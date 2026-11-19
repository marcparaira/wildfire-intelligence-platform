import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import MapView from "./MapView";
import { FRP_HIGH_COLOR, FRP_LOW_COLOR, FRP_UNAVAILABLE_COLOR, frpPosition, parseFrp } from "./frp";
import { DEFAULT_VIEW_BOUNDS, formatUtc, isRegionId, regions } from "./hotspots";
import type { Hotspot, HotspotResults, RegionId } from "./hotspots";

interface Filters {
  region: RegionId | "";
  dateFrom: string;
  dateTo: string;
}

function initialFilters(): Filters {
  const now = new Date();
  const from = new Date(now.getTime() - 6 * 24 * 60 * 60 * 1000);
  return {
    region: "",
    dateFrom: from.toISOString().slice(0, 10),
    dateTo: now.toISOString().slice(0, 10),
  };
}

function App() {
  const [draft, setDraft] = useState(initialFilters);
  const [filters, setFilters] = useState(draft);
  const [results, setResults] = useState<HotspotResults | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [formError, setFormError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [filtersOpen, setFiltersOpen] = useState(true);
  const [observationsOpen, setObservationsOpen] = useState(true);

  useEffect(() => {
    const controller = new AbortController();
    const params = new URLSearchParams({
      date_from: filters.dateFrom,
      date_to: filters.dateTo,
    });
    if (filters.region) {
      const [west, south, east, north] = regions[filters.region].bounds;
      params.set("west", String(west));
      params.set("south", String(south));
      params.set("east", String(east));
      params.set("north", String(north));
    }
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

  const hotspots = results?.items ?? [];
  const selected = hotspots.find((hotspot) => hotspot.id === selectedId);
  const areaBounds = filters.region ? regions[filters.region].bounds : null;

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!draft.dateFrom || !draft.dateTo || draft.dateFrom > draft.dateTo) {
      setFormError("Choose a valid date range. Start must be on or before end.");
      return;
    }
    setFormError("");
    setFilters({ ...draft });
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand"><h1>Wildfire Intelligence Platform</h1></div>
        <span className="header-source">NASA FIRMS · VIIRS NOAA-20 NRT</span>
      </header>

      <main className="workspace">
        <aside className="sidebar" aria-label="Filters" data-collapsed={!filtersOpen}>
          <div className="panel-heading panel-heading-toggle">
            <h2 hidden={!filtersOpen}>Filters</h2>
            <button type="button" className="panel-toggle" aria-label={filtersOpen ? "Collapse filters" : "Expand filters"} title={filtersOpen ? "Collapse filters" : "Expand filters"} aria-expanded={filtersOpen} aria-controls="filter-panel" onClick={() => setFiltersOpen((open) => !open)}>
              <svg aria-hidden="true" viewBox="0 0 16 16"><polyline points={filtersOpen ? "10.5 3 5.5 8 10.5 13" : "5.5 3 10.5 8 5.5 13"} /></svg>
            </button>
          </div>
          <form id="filter-panel" onSubmit={applyFilters} className="filters" hidden={!filtersOpen}>
            <label htmlFor="region">Geographic area</label>
            <select id="region" value={draft.region} onChange={(event) => {
              const value = event.target.value;
              if (value === "" || isRegionId(value)) {
                setDraft({ ...draft, region: value });
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
              <div><label htmlFor="date-from">From</label><input id="date-from" type="date" value={draft.dateFrom} onChange={(event) => setDraft({ ...draft, dateFrom: event.target.value })} /></div>
              <div><label htmlFor="date-to">To</label><input id="date-to" type="date" value={draft.dateTo} onChange={(event) => setDraft({ ...draft, dateTo: event.target.value })} /></div>
            </div>
            {formError && <p className="form-error" role="alert">{formError}</p>}
            <button type="submit" className="apply-button">Apply filters</button>
          </form>
        </aside>

        <section className="content" aria-label="Hotspot dashboard">
          <div className="map-toolbar">
            <div className="map-title"><h2>Hotspots</h2><span>{filters.region ? regions[filters.region].label : "All areas"}</span></div>
            <button className="refresh-button" type="button" onClick={() => setRefresh((value) => value + 1)}>Refresh data</button>
          </div>
          <div className="map-summary">
            <span>Detections <strong>{loading || !results ? "—" : hotspots.length.toLocaleString()}</strong></span>
            <span>Latest <strong>{hotspots[0] ? formatUtc(hotspots[0].observed_at) : "—"}</strong></span>
            {results?.truncated && <span className="limit-label">Result limit reached</span>}
          </div>
          {loading && <div className="map-message" role="status">Loading observations…</div>}
          {error && <div className="map-message map-error" role="alert">Could not load observations: {error}. Check the API and try again.</div>}
          {!loading && !error && hotspots.length === 0 && <div className="map-message" role="status">No detections for these filters. Try a wider area or date range.</div>}
          {results?.truncated && <div className="notice" role="status">Showing the most recent 5,000 detections. Narrow your filters to see the rest.</div>}
          <div className="map-wrap">
            <MapView hotspots={hotspots} bounds={areaBounds ?? DEFAULT_VIEW_BOUNDS} areaBounds={areaBounds} selectedId={selectedId} onSelect={(id) => { setSelectedId(id); setObservationsOpen(true); }} />
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
            {areaBounds && <span>Dashed outline: area filter</span>}
            <span>FRP is radiated power, not fire severity or danger. Detections are not fire perimeters.</span>
          </div>
        </section>

        <aside className="detail-column" aria-label="Observations" data-collapsed={!observationsOpen}>
          <div className="panel-heading panel-heading-toggle observation-heading">
            <button type="button" className="panel-toggle" aria-label={observationsOpen ? "Collapse observations" : "Expand observations"} title={observationsOpen ? "Collapse observations" : "Expand observations"} aria-expanded={observationsOpen} aria-controls="observation-panel" onClick={() => setObservationsOpen((open) => !open)}>
              <svg aria-hidden="true" viewBox="0 0 16 16"><polyline points={observationsOpen ? "5.5 3 10.5 8 5.5 13" : "10.5 3 5.5 8 10.5 13"} /></svg>
            </button>
            <h2 hidden={!observationsOpen}>Detection details</h2>
          </div>
          <div id="observation-panel" hidden={!observationsOpen}>
            <section className="details">
              {selected ? <DetectionDetails hotspot={selected} /> : <p className="detail-empty">Select a point on the map or an observation below.</p>}
            </section>
            <section className="recent">
              <div className="panel-heading"><h2>Recent detections</h2><span>{hotspots.length.toLocaleString()}</span></div>
              {hotspots.length > 0 ? <div className="recent-list">{hotspots.slice(0, 8).map((hotspot) => <button key={hotspot.id} type="button" className="recent-item" aria-pressed={hotspot.id === selectedId} onClick={() => setSelectedId(hotspot.id)}><i aria-hidden="true" /><span><strong>{hotspot.latitude.toFixed(3)}°, {hotspot.longitude.toFixed(3)}°</strong><small>{formatUtc(hotspot.observed_at)}</small></span></button>)}</div> : <p className="recent-empty">{loading ? "Loading observations…" : error ? "Observations unavailable." : "No observations to show."}</p>}
              {hotspots.length > 8 && <p className="recent-foot">Showing 8 most recent observations</p>}
            </section>
          </div>
        </aside>
      </main>
    </div>
  );
}

function DetectionDetails({ hotspot }: { hotspot: Hotspot }) {
  const frp = parseFrp(hotspot.attributes.frp);
  const fields = [
    ["Observed", formatUtc(hotspot.observed_at)],
    ["Coordinates", `${hotspot.latitude.toFixed(5)}°, ${hotspot.longitude.toFixed(5)}°`],
    ["Satellite", hotspot.satellite],
    ["Product", hotspot.source],
    ["Confidence", hotspot.attributes.confidence || "Not provided"],
    ["Fire radiative power", frp !== null ? `${frp} MW` : "Not provided"],
    ["Day / night", hotspot.attributes.daynight === "D" ? "Day" : hotspot.attributes.daynight === "N" ? "Night" : "Not provided"],
  ];
  return <div className="detail-content"><div className="detail-type">Satellite detection <span>#{hotspot.id}</span></div><dl>{fields.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl><p>Pixel center detected by NASA FIRMS, not a confirmed fire perimeter.</p></div>;
}

export default App;
