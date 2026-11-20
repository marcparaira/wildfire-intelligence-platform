import { useId } from "react";
import { formatNumber, formatRisk, formatWind, riskLevels } from "./conditions";
import type { RiskSummary } from "./conditions";
import { formatUtc } from "./hotspots";
import { growthChart, incidentStatuses, weatherOutlook } from "./incidents";
import type { IncidentDay, IncidentDetail, IncidentStatus, IncidentSummary, IncidentWeather } from "./incidents";

export function StatusBadge({ status }: { status: IncidentStatus }) {
  return (
    <span className="status-badge" data-status={status}>
      <i aria-hidden="true" style={{ backgroundColor: incidentStatuses[status].color }} />
      {incidentStatuses[status].label}
    </span>
  );
}

export function RiskBadge({ risk }: { risk: RiskSummary | null }) {
  if (!risk) return <span className="risk-badge" data-level="unknown">Risk unknown</span>;
  return (
    <span className="risk-badge" data-level={risk.level} title={`Fire risk score ${risk.score} of 12`}>
      <i aria-hidden="true" style={{ backgroundColor: riskLevels[risk.level].color }} />
      {formatRisk(risk)}
    </span>
  );
}

function formatArea(km2: number): string {
  return `${km2 < 10 ? km2.toFixed(1) : Math.round(km2).toLocaleString("en-US")} km²`;
}

interface IncidentPanelProps {
  items: IncidentSummary[];
  truncated: boolean;
  loading: boolean;
  error: string;
  selectedId: string | null;
  showOnMap: boolean;
  onShowOnMapChange: (show: boolean) => void;
  onSelect: (incident: IncidentSummary) => void;
}

export function IncidentPanel({
  items,
  truncated,
  loading,
  error,
  selectedId,
  showOnMap,
  onShowOnMapChange,
  onSelect,
}: IncidentPanelProps) {
  return (
    <section className="incidents" aria-label="Fire incidents">
      <div className="panel-heading">
        <h2>Incidents</h2>
        <span>{loading && items.length === 0 ? "—" : items.length.toLocaleString("en-US")}</span>
      </div>
      <label className="layer-toggle">
        <input type="checkbox" checked={showOnMap} onChange={(event) => onShowOnMapChange(event.target.checked)} />
        Show incidents on map
      </label>
      {error && <p className="incident-message map-error" role="alert">Could not load incidents: {error}.</p>}
      {items.length > 0 ? (
        <ul className="incident-list">
          {items.map((incident) => (
            <li key={incident.incident_id}>
              <button
                type="button"
                className="incident-item"
                aria-pressed={incident.incident_id === selectedId}
                onClick={() => onSelect(incident)}
              >
                <span className="incident-item-head">
                  <StatusBadge status={incident.status} />
                  <RiskBadge risk={incident.peak_risk} />
                </span>
                <span className="incident-item-figures">
                  <strong>{formatArea(incident.footprint_km2)}</strong>
                  <span>{incident.detection_count.toLocaleString("en-US")} detections</span>
                </span>
                <small>Last seen {formatUtc(incident.last_observed_at)}</small>
              </button>
            </li>
          ))}
        </ul>
      ) : (
        !error && <p className="incident-message">{loading ? "Loading incidents…" : "No active incidents in this area."}</p>
      )}
      {truncated && <p className="incident-message">Showing the most recently active incidents only.</p>}
    </section>
  );
}

const CHART_WIDTH = 262;
const CHART_HEIGHT = 96;

export function GrowthChart({ daily }: { daily: IncidentDay[] }) {
  const titleId = useId();
  if (daily.length === 0) return <p className="detail-note">No daily activity recorded.</p>;
  const chart = growthChart(daily, CHART_WIDTH, CHART_HEIGHT);
  const first = daily[0];
  const last = daily[daily.length - 1];
  const summary =
    `${daily.length} ${daily.length === 1 ? "day" : "days"} from ${first.day} to ${last.day}; ` +
    `up to ${chart.maxDetections.toLocaleString("en-US")} detections a day; ` +
    `footprint ${formatArea(last.footprint_km2)} at the end.`;
  return (
    <figure className="growth-chart">
      <svg viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT + 14}`} role="img" aria-labelledby={titleId}>
        <title id={titleId}>{`Growth over time: ${summary}`}</title>
        <line className="chart-axis" x1={0} y1={CHART_HEIGHT} x2={CHART_WIDTH} y2={CHART_HEIGHT} />
        {chart.bars.map((bar) => (
          <rect key={bar.day} className="chart-bar" x={bar.x} y={bar.y} width={bar.width} height={bar.height}>
            <title>{`${bar.day}: ${bar.detections} detections`}</title>
          </rect>
        ))}
        <polyline className="chart-line" points={chart.footprintLine} />
        <text className="chart-label" x={0} y={CHART_HEIGHT + 12}>{first.day.slice(5)}</text>
        {daily.length > 1 && <text className="chart-label" x={CHART_WIDTH} y={CHART_HEIGHT + 12} textAnchor="end">{last.day.slice(5)}</text>}
      </svg>
      <figcaption>
        <span><i className="chart-key-bar" aria-hidden="true" />Detections per day (max {chart.maxDetections.toLocaleString("en-US")})</span>
        <span><i className="chart-key-line" aria-hidden="true" />Cumulative footprint (max {formatArea(chart.maxFootprintKm2)})</span>
      </figcaption>
      <details className="chart-data">
        <summary>Daily data</summary>
        <table>
          <thead>
            <tr><th scope="col">Day (UTC)</th><th scope="col">Detections</th><th scope="col">New cells</th><th scope="col">Footprint</th></tr>
          </thead>
          <tbody>
            {daily.map((day) => (
              <tr key={day.day}>
                <th scope="row">{day.day}</th>
                <td>{day.detections.toLocaleString("en-US")}</td>
                <td>{day.new_cells.toLocaleString("en-US")}</td>
                <td>{formatArea(day.footprint_km2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

function hourLabel(timestamp: string): string {
  return `${new Date(timestamp).toISOString().slice(11, 16)}`;
}

export function WeatherContext({ weather }: { weather: IncidentWeather | null }) {
  const { current, upcoming } = weatherOutlook(weather?.hours ?? []);
  if (!weather || !current) {
    return <p className="detail-note">No forecast is available for this incident's area.</p>;
  }
  return (
    <div className="weather-context">
      <dl>
        <div><dt>Now ({formatUtc(current.valid_at)})</dt><dd>{formatNumber(current.temperature_c, "°C")}</dd></div>
        <div><dt>Wind</dt><dd>{formatWind(current.wind_speed_kmh, current.wind_direction_degrees)}</dd></div>
        <div><dt>Precipitation</dt><dd>{formatNumber(current.precipitation_mm, "mm")}</dd></div>
      </dl>
      {upcoming.length > 0 && (
        <table className="outlook" aria-label="Next 24 hours, every 3 hours">
          <thead>
            <tr><th scope="col">UTC</th><th scope="col">Temp.</th><th scope="col">Wind</th><th scope="col">Rain</th></tr>
          </thead>
          <tbody>
            {upcoming.map((hour) => (
              <tr key={hour.valid_at}>
                <th scope="row">{hourLabel(hour.valid_at)}</th>
                <td>{formatNumber(hour.temperature_c, "°C", 0)}</td>
                <td>{formatWind(hour.wind_speed_kmh, hour.wind_direction_degrees)}</td>
                <td>{formatNumber(hour.precipitation_mm, "mm")}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="detail-note">Open-Meteo forecast for the surrounding grid cell, retrieved {formatUtc(weather.generated_at)}.</p>
    </div>
  );
}

interface IncidentDetailsProps {
  summary: IncidentSummary | null;
  incident: IncidentDetail | null;
  loading: boolean;
  error: string;
}

export function IncidentDetails({ summary, incident, loading, error }: IncidentDetailsProps) {
  const shown: IncidentSummary | null = incident ?? summary;
  if (!shown) {
    if (error) return <p className="detail-empty" role="alert">Could not load incident: {error}.</p>;
    return <p className="detail-empty">{loading ? "Loading incident…" : "Incident unavailable."}</p>;
  }
  const status = shown.previous_status && shown.previous_status !== shown.status
    ? `${incidentStatuses[shown.status].label} (was ${incidentStatuses[shown.previous_status].label.toLowerCase()})`
    : incidentStatuses[shown.status].label;
  const fields: [string, string][] = [
    ["Status", status],
    ["First observed", formatUtc(shown.first_observed_at)],
    ["Last observed", formatUtc(shown.last_observed_at)],
    ["Detections", shown.detection_count.toLocaleString("en-US")],
    ["Footprint", `${formatArea(shown.footprint_km2)} (${shown.footprint_cells.toLocaleString("en-US")} cells)`],
    ["Maximum FRP", formatNumber(shown.max_frp_mw, "MW")],
    ["Total FRP", formatNumber(shown.total_frp_mw, "MW")],
    ["Products", shown.products.join(", ") || "—"],
    ["Peak fire risk (24 h)", shown.peak_risk ? formatRisk(shown.peak_risk) : "Unknown"],
  ];
  return (
    <div className="detail-content incident-detail">
      <div className="detail-type">Fire incident <StatusBadge status={shown.status} /></div>
      <dl>{fields.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>
      {shown.merged_into && <p>Merged into incident {shown.merged_into.slice(0, 8)}.</p>}
      <h3>Growth over time</h3>
      {incident ? <GrowthChart daily={incident.daily} /> : <p className="detail-note" role={error ? "alert" : "status"}>{error ? `Could not load history: ${error}.` : "Loading history…"}</p>}
      <h3>Weather context</h3>
      {incident ? <WeatherContext weather={incident.weather} /> : <p className="detail-note">{error ? "Weather unavailable." : "Loading weather…"}</p>}
      <p>Detections clustered by space and time; the footprint counts ~5 km² grid cells with detections, not a mapped fire perimeter. Peak risk is the highest risk among the incident's detections in its last 24 hours of activity.</p>
    </div>
  );
}
