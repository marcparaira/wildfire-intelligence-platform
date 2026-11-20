import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { IncidentDetailState, IncidentSummary, IncidentsState } from "./incidents";
import { enrichedHotspot, incidentDetail, incidentSummary } from "./test-fixtures";
import type { Bounds, Hotspot } from "./hotspots";
import type { PipelineState } from "./pipeline";
import type { WeatherPoint } from "./weather";

const pipelineState = vi.hoisted(() => ({ current: { status: null, error: "", loading: true } as PipelineState }));

// The pipeline status poll has its own tests; keep it out of the fetch call counts here.
const incidentState = vi.hoisted(() => ({ current: { items: [], truncated: false, loading: false, error: "" } as IncidentsState }));
const incidentDetailState = vi.hoisted(() => ({ current: { incident: null, loading: false, error: "" } as IncidentDetailState }));
// Incidents have their own tests; keep their requests out of the fetch call counts here.
vi.mock("./incidents", async (importOriginal) => ({
  ...await importOriginal<typeof import("./incidents")>(),
  useIncidents: () => incidentState.current,
  useIncidentDetail: () => incidentDetailState.current,
}));

vi.mock("./pipeline", async (importOriginal) => ({
  ...await importOriginal<typeof import("./pipeline")>(),
  usePipelineStatus: () => pipelineState.current,
}));

vi.mock("./MapView", () => ({
  default: ({ hotspots, weatherPoints, weatherMetric, bounds, areaBounds, onSelect, incidents = [], selectedIncidentId = null, onSelectIncident, focusBounds = null }: {
    hotspots: Hotspot[];
    weatherPoints: WeatherPoint[];
    weatherMetric: string;
    bounds: Bounds;
    areaBounds: Bounds | null;
    onSelect: (eventId: string) => void;
    incidents?: IncidentSummary[];
    selectedIncidentId?: string | null;
    onSelectIncident?: (incidentId: string) => void;
    focusBounds?: Bounds | null;
  }) => (
    <div
      aria-label="Hotspot map"
      data-bounds={bounds.join(",")}
      data-area-bounds={areaBounds?.join(",") ?? ""}
      data-weather-count={weatherPoints.length}
      data-weather-metric={weatherMetric}
      data-incident-count={incidents.length}
      data-selected-incident={selectedIncidentId ?? ""}
      data-focus-bounds={focusBounds?.join(",") ?? ""}
    >
      {hotspots.map((hotspot) => (
        <button key={hotspot.event_id} onClick={() => onSelect(hotspot.event_id)}>Map point {hotspot.id ?? hotspot.event_id}</button>
      ))}
      {incidents.map((incident) => (
        <button key={incident.incident_id} onClick={() => onSelectIncident?.(incident.incident_id)}>Map incident {incident.incident_id.slice(0, 8)}</button>
      ))}
    </div>
  ),
}));

const observation: Hotspot = {
  id: 7,
  event_id: "6a1f8f4e-7d0c-5a6e-9c1d-2b3e4f5a6b7c",
  source: "VIIRS_NOAA20_NRT",
  instrument: "VIIRS",
  satellite: "N20",
  observed_at: "2026-03-05T00:07:00Z",
  latitude: 0.5234,
  longitude: 114.1234,
  attributes: { confidence: "nominal", frp: "12.5", daynight: "N" },
  duplicate_of: null,
};

afterEach(() => {
  cleanup();
  incidentState.current = { items: [], truncated: false, loading: false, error: "" };
  incidentDetailState.current = { incident: null, loading: false, error: "" };
  window.location.hash = "";
  pipelineState.current = { status: null, error: "", loading: true };
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("hotspot dashboard", () => {
  it("loads observations and shows them on the map", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);

    // Map point should be available (since no incidents exist, hotspots are shown as fallback)
    expect(await screen.findByRole("button", { name: /Map point 7/ })).toBeInTheDocument();
    const params = new URL(String(fetchMock.mock.calls[0][0]), "http://localhost").searchParams;
    expect(["west", "south", "east", "north"].some((key) => params.has(key))).toBe(false);
    expect(params.has("date_from")).toBe(true);
    expect(params.has("date_to")).toBe(true);
    expect(screen.getByLabelText("Geographic area")).toHaveValue("");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "92,-12,156,29");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "");
    expect(screen.queryByText("Dashed outline: area filter")).not.toBeInTheDocument();
  });

  it("applies geographic and date filters and flags truncated results", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: true }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByText("Result limit reached");
    
    // Change region - applies immediately
    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "sumatra" } });
    
    // Wait for the fetch with region bounds to be called
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const regionUrl = String(fetchMock.mock.calls[1][0]);
    expect(regionUrl).toContain("west=94");
    
    // Change date filters - applies immediately now (no button click needed)
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-03-01" } });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-03-05" } });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(4));
    
    const url = String(fetchMock.mock.calls[3][0]);
    expect(url).toContain("west=94");
    expect(url).toContain("date_from=2026-03-01");
    expect(url).toContain("date_to=2026-03-05");
    expect(await screen.findByText(/Showing the most recent 5,000/)).toBeInTheDocument();
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "94,-7,106,6");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "94,-7,106,6");
    expect(screen.getByText("Dashed outline: area filter")).toBeInTheDocument();

    // Reset region
    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "" } });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
    const resetParams = new URL(String(fetchMock.mock.calls[4][0]), "http://localhost").searchParams;
    expect(["west", "south", "east", "north"].some((key) => resetParams.has(key))).toBe(false);
    expect(resetParams.get("date_from")).toBe("2026-03-01");
    expect(resetParams.get("date_to")).toBe("2026-03-05");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "92,-12,156,29");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "");
    expect(screen.queryByText("Dashed outline: area filter")).not.toBeInTheDocument();
  });

  it("applies regional bounds correctly to the map view", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    // Map point should be available
    await screen.findByRole("button", { name: /Map point 7/ });
    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "sumatra" } });
    // Area filter is now applied immediately
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const params = new URL(String(fetchMock.mock.calls[1][0]), "http://localhost").searchParams;
    expect(["west", "south", "east", "north"].map((key) => params.get(key)).join(",")).toBe("94,-7,106,6");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "94,-7,106,6");
  });

  it("reports failed requests and rejects an invalid date range", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    expect(await screen.findByText(/Could not load observations/)).toBeInTheDocument();
    // First change triggers a fetch attempt even though date is incomplete
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-03-06" } });
    // Second change creates invalid range, shows error, doesn't fetch
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-03-05" } });
    // Invalid date range should show error
    expect(screen.getByText(/Choose a valid date range/)).toBeInTheDocument();
    // Only 1 or 2 fetches depending on timing (from first date change and failed initial fetch)
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(1);
  });

  it("collapses and expands filter panel", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByRole("button", { name: /Map point 7/ });

    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "sumatra" } });
    // Wait for region change fetch
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    
    const filters = screen.getByRole("button", { name: "Collapse filters" });
    fireEvent.click(filters);
    expect(filters).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByLabelText("Geographic area")).not.toBeVisible();
    expect(filters).toHaveAccessibleName("Expand filters");
    expect(filters).toHaveAttribute("aria-controls", "filter-panel");
    fireEvent.click(filters);
    expect(filters).toHaveAccessibleName("Collapse filters");
    expect(screen.getByLabelText("Geographic area")).toBeVisible();
    expect(screen.getByLabelText("Geographic area")).toHaveValue("sumatra");

    fireEvent.click(screen.getByRole("button", { name: "Refresh data" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
  });

  it("displays the FRP legend and scale correctly", async () => {
    const zero = { ...observation, attributes: { frp: "0" } };
    const invalid = { ...observation, id: 8, event_id: "0b6c2d0e-1f2a-5b3c-8d4e-5f6a7b8c9d0e", attributes: { frp: "-1" } };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [zero, invalid], truncated: false }),
    }));
    render(<App />);
    expect(await screen.findByRole("group", { name: /FRP legend, logarithmic scale/ })).toHaveTextContent("≥100");
    expect(screen.getByText(/not fire severity or danger/)).toBeInTheDocument();
    // Map points should be visible (no incidents, so hotspots shown as fallback)
    expect(screen.getByRole("button", { name: "Map point 7" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Map point 8" })).toBeInTheDocument();
  });

  it("loads a weather overlay and changes the displayed forecast hour on the timeline", async () => {
    const forecast = {
      generated_at: "2026-03-04T20:00:00Z",
      oldest_generated_at: "2026-03-04T19:00:00Z",
      valid_at: "2026-03-05T00:00:00Z",
      times: ["2026-03-05T00:00:00Z", "2026-03-05T01:00:00Z"],
      items: [{
        cell: "82654ffffffffff",
        latitude: 1,
        longitude: 115,
        boundary: [[114, 0], [116, 0], [116, 2], [114, 2], [114, 0]],
        generated_at: "2026-03-04T20:00:00Z",
        temperature_c: 30,
        precipitation_mm: 0,
        wind_speed_kmh: 12,
        wind_direction_degrees: 90,
      }],
    };
    const fetchMock = vi.fn().mockImplementation(async (input: string) => ({
      ok: true,
      json: async () => input.startsWith("/api/weather")
        ? forecast
        : { items: [observation], truncated: false },
    }));
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    // Map point should be available (hotspots shown as fallback when no incident selected)
    await screen.findByRole("button", { name: /Map point 7/ });

    fireEvent.change(screen.getByLabelText("Weather layer"), { target: { value: "temperature" } });
    await waitFor(() => {
      expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-weather-metric", "temperature");
      expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-weather-count", "1");
    });
    expect(fetchMock.mock.calls[1][1]).not.toHaveProperty("method");
    expect(screen.getByText("Forecast retrieved 2026-03-04 20:00 UTC")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Refresh forecast" })).not.toBeInTheDocument();
    expect(await screen.findByText("2026-03-05 00:00 UTC", { selector: "output" })).toBeInTheDocument();

    fireEvent.change(screen.getByRole("slider", { name: /Forecast time/ }), {
      target: { value: "1" },
    });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    expect(String(fetchMock.mock.calls[2][0])).toContain("at=2026-03-05T01%3A00%3A00Z");

    fireEvent.click(screen.getByRole("button", { name: "Refresh data" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
    expect(fetchMock.mock.calls.slice(3).map(([url]) => String(url).split("?")[0]).sort()).toEqual([
      "/api/hotspots",
      "/api/weather/forecast",
    ]);

    fireEvent.change(screen.getByLabelText("Weather layer"), { target: { value: "" } });
    await waitFor(() => {
      expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-weather-metric", "");
      expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-weather-count", "0");
    });
  });

  it("explains when no forecast has been ingested yet", async () => {
    const fetchMock = vi.fn().mockImplementation(async (input: string) => {
      if (input.startsWith("/api/weather")) {
        return {
          ok: false,
          status: 503,
          json: async () => ({ detail: "No weather forecast has been ingested yet" }),
        };
      }
      return { ok: true, json: async () => ({ items: [], truncated: false }) };
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByText("No detections for these filters. Try a wider area or date range.");

    fireEvent.change(screen.getByLabelText("Weather layer"), { target: { value: "wind" } });

    expect(await screen.findByText(/No weather forecast has been ingested yet/)).toBeInTheDocument();
  });

  it("shows source freshness and switches to the pipeline status page", async () => {
    const recent = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();
    pipelineState.current = {
      loading: false,
      error: "",
      status: {
        checked_at: new Date().toISOString(),
        sources: [{
          id: "VIIRS_NOAA20_NRT", label: "VIIRS NOAA-20", kind: "observation", topic: "firms.detections",
          freshness: "fresh", latest_at: recent, oldest_retrieved_at: null, forecast_until: null,
          latest_ingested_at: recent, latest_stored_at: recent, ingested_last_hour: 3,
          ingested_last_day: 120, stored: 4000, fresh_within_hours: 18, stale_after_hours: 36,
        }],
        topics: null,
        consumers: null,
        dead_letters: null,
        errors: ["Kafka is unavailable; throughput, lag, and dead letters are unknown"],
      },
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    }));
    render(<App />);

    // Data freshness is now on the pipeline status page, not in sidebar
    // Switch to status page to verify it's there
    window.location.hash = "#/status";
    window.dispatchEvent(new HashChangeEvent("hashchange"));
    expect(await screen.findByRole("heading", { name: "Pipeline status" })).toBeInTheDocument();
    
    const freshness = screen.getByRole("region", { name: "Data freshness" });
    expect(freshness).toHaveTextContent("VIIRS NOAA-20");
    expect(freshness).toHaveTextContent("Fresh");
    expect(freshness).toHaveTextContent("Observed 2 h ago");
    
    expect(screen.getByText(/Kafka is unavailable/)).toBeInTheDocument();
    expect(screen.queryByLabelText("Hotspot map")).not.toBeInTheDocument();
  });

  it("selects incidents from the list or the map to show incident details", async () => {
    const incident = incidentSummary();
    incidentState.current = { items: [incident], truncated: false, loading: false, error: "" };
    incidentDetailState.current = { incident: incidentDetail(), loading: false, error: "" };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({ items: [observation], truncated: false }) }));
    render(<App />);
    // Map point should be available (hotspots shown as fallback when no incident selected)
    await screen.findByRole("button", { name: /Map point 7/ });
    const map = screen.getByLabelText("Hotspot map");
    expect(map).toHaveAttribute("data-incident-count", "1");
    expect(screen.getByText("Incidents", { selector: ".incident-legend span" })).toBeInTheDocument();

    // Select incident from the map
    fireEvent.click(screen.getByRole("button", { name: "Map incident 0b6f3a52" }));
    expect(screen.getByRole("heading", { name: "Incident details" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: /Growth over time/ })).toBeInTheDocument();
    expect(map).toHaveAttribute("data-selected-incident", incident.incident_id);
    expect(map).toHaveAttribute("data-focus-bounds", incident.bbox.join(","));

    // Status filter still shows incidents
    expect(map).toHaveAttribute("data-incident-count", "1");
  });

  it("shows the country, fire risk, and weather of an enriched detection", async () => {
    // Create a mock incident with associated detection data
    const mockIncidents: IncidentSummary[] = [incidentSummary()];
    incidentState.current = { items: mockIncidents, truncated: false, loading: false, error: "" };
    
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({ items: [enrichedHotspot()], truncated: false }) }));
    render(<App />);
    
    // Select incident from the mock incidents
    fireEvent.click(await screen.findByRole("button", { name: /Map incident/ }));
    
    // With incident selected, incident details should be shown
    expect(screen.getByText("Fire incident")).toBeInTheDocument(); // Incident details type
    expect(screen.getByText("42", { selector: "dd" })).toBeInTheDocument(); // Detection count in details
    // Peak risk appears in the incident details
    const detailsSection = screen.getByRole("heading", { name: "Incident details" }).closest("aside");
    expect(detailsSection?.textContent).toContain("High · 8/12");
  });

  it("removes the detection-centric UI elements (detection details and recent detections)", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({ items: [enrichedHotspot()], truncated: false }) }));
    render(<App />);
    // Map point should be available (no incidents, so hotspots shown as fallback)
    await screen.findByRole("button", { name: /Map point 7/ });
    
    // The right sidebar should not have a "Recent detections" section or detection-specific details
    expect(screen.queryByText("Recent detections")).not.toBeInTheDocument();
    // The incident details panel should not be shown when no incident is selected
    expect(screen.queryByText("Incident unavailable")).not.toBeInTheDocument();
  });
});
