import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import MapView from "./MapView";
import { formatUtc, regions } from "./hotspots";
import type { Hotspot, HotspotResults } from "./hotspots";

interface Filters {
  region: string;
  dateFrom: string;
  dateTo: string;
}

function initialFilters(): Filters {
  const now = new Date();
  const from = new Date(now.getTime() - 6 * 24 * 60 * 60 * 1000);
  return {
    region: "southeastAsia",
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
    const [west, south, east, north] = regions[filters.region].bounds;
    const params = new URLSearchParams({
      west: String(west),
      south: String(south),
      east: String(east),
      north: String(north),
      date_from: filters.dateFrom,
      date_to: filters.dateTo,
    });
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
        <div className="brand"><h1>Wildfire intelligence</h1><span>Hotspot monitoring</span></div>
        <div className="topbar-actions">
          <button type="button" className="toolbar-button" aria-expanded={filtersOpen} aria-controls="filter-panel" onClick={() => setFiltersOpen((open) => !open)}>
            Filters
          </button>
          <button type="button" className="toolbar-button" aria-expanded={observationsOpen} aria-controls="observation-panel" onClick={() => setObservationsOpen((open) => !open)}>
            Observations
          </button>
        </div>
        <span className="header-source">NASA FIRMS · VIIRS NOAA-20 NRT</span>
      </header>

      <main className="workspace">
        <aside id="filter-panel" className="sidebar" aria-label="Filters" hidden={!filtersOpen}>
          <div className="panel-heading"><h2>Filters</h2></div>
          <form onSubmit={applyFilters} className="filters">
            <label htmlFor="region">Geographic area</label>
            <select id="region" value={draft.region} onChange={(event) => setDraft({ ...draft, region: event.target.value })}>
              {Object.entries(regions).map(([key, region]) => <option key={key} value={key}>{region.label}</option>)}
            </select>
            <p className="field-note">Bounding box, not an administrative boundary.</p>
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
            <div className="map-title"><h2>Hotspots</h2><span>{regions[filters.region].label}</span></div>
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
            <MapView hotspots={hotspots} bounds={regions[filters.region].bounds} selectedId={selectedId} onSelect={(id) => { setSelectedId(id); setObservationsOpen(true); }} />
          </div>
          <div className="map-footer">
            <span className="legend" aria-label="Map legend"><i aria-hidden="true" /> Satellite detection · NASA FIRMS VIIRS NOAA-20 NRT</span>
            <span>Observations are not confirmed fire perimeters.</span>
          </div>
        </section>

        <aside id="observation-panel" className="detail-column" aria-label="Observations" hidden={!observationsOpen}>
          <section className="details">
            <div className="panel-heading"><h2>Detection details</h2></div>
            {selected ? <DetectionDetails hotspot={selected} /> : <p className="detail-empty">Select a point on the map or an observation below.</p>}
          </section>
          <section className="recent">
            <div className="panel-heading"><h2>Recent detections</h2><span>{hotspots.length.toLocaleString()}</span></div>
            {hotspots.length > 0 ? <div className="recent-list">{hotspots.slice(0, 8).map((hotspot) => <button key={hotspot.id} type="button" className="recent-item" aria-pressed={hotspot.id === selectedId} onClick={() => setSelectedId(hotspot.id)}><i aria-hidden="true" /><span><strong>{hotspot.latitude.toFixed(3)}°, {hotspot.longitude.toFixed(3)}°</strong><small>{formatUtc(hotspot.observed_at)}</small></span></button>)}</div> : <p className="recent-empty">{loading ? "Loading observations…" : error ? "Observations unavailable." : "No observations to show."}</p>}
            {hotspots.length > 8 && <p className="recent-foot">Showing 8 most recent observations</p>}
          </section>
        </aside>
      </main>
    </div>
  );
}

function DetectionDetails({ hotspot }: { hotspot: Hotspot }) {
  const fields = [
    ["Observed", formatUtc(hotspot.observed_at)],
    ["Coordinates", `${hotspot.latitude.toFixed(5)}°, ${hotspot.longitude.toFixed(5)}°`],
    ["Satellite", hotspot.satellite],
    ["Product", hotspot.source],
    ["Confidence", hotspot.attributes.confidence || "Not provided"],
    ["Fire radiative power", hotspot.attributes.frp ? `${hotspot.attributes.frp} MW` : "Not provided"],
    ["Day / night", hotspot.attributes.daynight === "D" ? "Day" : hotspot.attributes.daynight === "N" ? "Night" : "Not provided"],
  ];
  return <div className="detail-content"><div className="detail-type">Satellite detection <span>#{hotspot.id}</span></div><dl>{fields.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl><p>Pixel center detected by NASA FIRMS, not a confirmed fire perimeter.</p></div>;
}

export default App;
