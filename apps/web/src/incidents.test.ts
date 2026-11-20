import { cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { compassPoint, countryName, formatRisk, formatWind } from "./conditions";
import {
  growthChart,
  incidentMarkerRadius,
  isIncidentSummary,
  sortIncidents,
  useIncidentDetail,
  useIncidents,
  weatherOutlook,
} from "./incidents";
import { incidentDays, incidentDetail, incidentSummary } from "./test-fixtures";

function jsonResponse(body: unknown, status = 200) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("incident helpers", () => {
  it("orders incidents by last activity, then by ID", () => {
    const older = incidentSummary({ incident_id: "a", last_observed_at: "2026-03-04T00:00:00Z" });
    const newerB = incidentSummary({ incident_id: "b", last_observed_at: "2026-03-05T00:00:00Z" });
    const newerC = incidentSummary({ incident_id: "c", last_observed_at: "2026-03-05T00:00:00Z" });
    expect(sortIncidents([older, newerC, newerB]).map((item) => item.incident_id)).toEqual(["b", "c", "a"]);
  });

  it("rejects malformed incidents", () => {
    expect(isIncidentSummary(incidentSummary())).toBe(true);
    expect(isIncidentSummary(incidentSummary({ peak_risk: null }))).toBe(true);
    expect(isIncidentSummary({ ...incidentSummary(), status: "burning" })).toBe(false);
    expect(isIncidentSummary({ ...incidentSummary(), bbox: [1, 2, 3] })).toBe(false);
    expect(isIncidentSummary({ ...incidentSummary(), peak_risk: { score: 3, level: "severe" } })).toBe(false);
    expect(isIncidentSummary({ event_id: "x" })).toBe(false);
  });

  it("maps daily activity to bars and a cumulative footprint line", () => {
    const chart = growthChart(incidentDays, 300, 100);
    expect(chart.maxDetections).toBe(30);
    expect(chart.maxFootprintKm2).toBe(31.2);
    expect(chart.bars.map((bar) => [bar.day, bar.height])).toEqual([
      ["2026-03-03", 13.3],
      ["2026-03-04", 100],
      ["2026-03-05", 26.7],
    ]);
    expect(chart.bars[1]).toMatchObject({ x: 115, y: 0, width: 70 });
    expect(chart.footprintLine).toBe("50,66.7 150,16.7 250,0");
  });

  it("draws an empty chart without dividing by zero", () => {
    const quiet = growthChart([{ ...incidentDays[0], detections: 0, footprint_km2: 0 }], 100, 50);
    expect(quiet.bars[0].height).toBe(0);
    expect(quiet.footprintLine).toBe("50,50");
    expect(growthChart([], 100, 50)).toEqual({ bars: [], footprintLine: "", maxDetections: 0, maxFootprintKm2: 0 });
  });

  it("picks the current hour and every third hour up to a day ahead", () => {
    const hours = incidentDetail().weather?.hours ?? [];
    const outlook = weatherOutlook([...hours].reverse());
    expect(outlook.current?.valid_at).toBe("2026-03-05T07:00:00.000Z");
    expect(outlook.upcoming.map((item) => item.valid_at.slice(11, 13))).toEqual(["10", "13", "16", "19", "22", "01", "04", "07"]);
    expect(weatherOutlook([])).toEqual({ current: null, upcoming: [] });
  });

  it("grows markers with the footprint up to a cap", () => {
    expect(incidentMarkerRadius(0)).toBe(7);
    expect(incidentMarkerRadius(5)).toBe(10);
    expect(incidentMarkerRadius(10_000)).toBe(18);
  });

  it("formats countries, wind, and risk", () => {
    expect(countryName("IDN")).toBe("Indonesia");
    expect(countryName("XYZ")).toBe("XYZ");
    expect(countryName(null)).toBe("Outside the selected countries");
    expect(compassPoint(0)).toBe("N");
    expect(compassPoint(350)).toBe("N");
    expect(compassPoint(-90)).toBe("W");
    expect(formatWind(13.6, 225)).toBe("14 km/h from SW");
    expect(formatWind(13.6, null)).toBe("14 km/h");
    expect(formatWind(null, 90)).toBe("—");
    expect(formatRisk({ score: 10, level: "extreme" })).toBe("Extreme · 10/12");
  });
});

describe("useIncidents", () => {
  it("loads the area's incidents, sorted, and reloads on an interval", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const older = incidentSummary({ incident_id: "a", last_observed_at: "2026-03-01T00:00:00Z" });
    const newer = incidentSummary({ incident_id: "b" });
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ items: [older, newer], truncated: true }));
    vi.stubGlobal("fetch", fetchMock);

    const { result } = renderHook(() => useIncidents("west=94&south=-7&east=106&north=6", 0, 1000));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(String(fetchMock.mock.calls[0][0])).toBe("/api/incidents?west=94&south=-7&east=106&north=6");
    expect(result.current.items.map((item) => item.incident_id)).toEqual(["b", "a"]);
    expect(result.current.truncated).toBe(true);

    fetchMock.mockResolvedValue(jsonResponse({ detail: "boom" }, 503));
    await vi.advanceTimersByTimeAsync(1000);
    await waitFor(() => expect(result.current.error).toBe("Request failed (503)"));
    expect(result.current.items).toHaveLength(2);
  });

  it("requests every area without a query and reports malformed responses", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ items: [{ event_id: "x" }], truncated: false }));
    vi.stubGlobal("fetch", fetchMock);
    const { result } = renderHook(() => useIncidents(""));
    await waitFor(() => expect(result.current.error).toBe("Unexpected response"));
    expect(String(fetchMock.mock.calls[0][0])).toBe("/api/incidents");
  });
});

describe("useIncidentDetail", () => {
  it("loads the selected incident and reloads when its revision changes", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(incidentDetail()));
    vi.stubGlobal("fetch", fetchMock);
    const initialProps: { id: string | null; revision: number | null } = {
      id: "0b6f3a52-1c1e-5d55-9a7e-1f0c6b8f2a10",
      revision: 3,
    };
    const { result, rerender } = renderHook(({ id, revision }) => useIncidentDetail(id, revision), { initialProps });
    await waitFor(() => expect(result.current.incident?.daily).toHaveLength(3));
    expect(String(fetchMock.mock.calls[0][0])).toBe("/api/incidents/0b6f3a52-1c1e-5d55-9a7e-1f0c6b8f2a10");

    rerender({ id: "0b6f3a52-1c1e-5d55-9a7e-1f0c6b8f2a10", revision: 4 });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.incident).not.toBeNull();

    rerender({ id: null, revision: null });
    await waitFor(() => expect(result.current.incident).toBeNull());
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("reports an unknown incident", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse({ detail: "Incident not found" }, 404)));
    const { result } = renderHook(() => useIncidentDetail("missing", null));
    await waitFor(() => expect(result.current.error).toBe("Incident not found"));
    expect(result.current.incident).toBeNull();
  });
});
