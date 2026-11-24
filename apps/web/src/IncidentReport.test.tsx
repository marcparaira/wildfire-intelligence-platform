import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { IncidentReport } from "./IncidentReport";
import type { Bounds, Hotspot } from "./hotspots";
import type { SatelliteScene } from "./imagery";
import { enrichedHotspot, FALSE_COLOR, incidentDetail, satelliteScene, sentinelScene } from "./test-fixtures";

vi.mock("./ImageryMap", () => ({
  default: ({ scenes, incidentBounds, detections, opacity }: { scenes: SatelliteScene[]; incidentBounds: Bounds; detections: Hotspot[]; opacity: number }) => (
    <div
      aria-label="Satellite imagery map"
      data-scenes={scenes.map((scene) => scene.scene_id).join(",")}
      data-bounds={incidentBounds.join(",")}
      data-detections={detections.length}
      data-opacity={opacity}
    />
  ),
}));

const INCIDENT = incidentDetail();

interface Responses {
  incident?: unknown;
  imagery?: unknown;
  hotspots?: unknown;
  status?: number;
}

function stubApi({ incident = INCIDENT, imagery, hotspots = { items: [], truncated: false }, status = 200 }: Responses) {
  const fetchMock = vi.fn().mockImplementation(async (input: string) => {
    const path = String(input).split("?")[0];
    const body = path.endsWith("/imagery") ? imagery : path.startsWith("/api/hotspots") ? hotspots : incident;
    const failed = path.endsWith("/imagery") ? status : 200;
    return { ok: failed < 400, status: failed, json: async () => body };
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function imagery(scenes: SatelliteScene[]) {
  return { incident_id: INCIDENT.incident_id, first_day: "2026-03-03", last_day: "2026-03-05", scenes };
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("incident report", () => {
  it("shows details, evolution, weather, and the latest imagery with that day's detections", async () => {
    const fetchMock = stubApi({
      imagery: imagery([
        satelliteScene({ scene_id: "old", acquisition_date: "2026-03-04" }),
        satelliteScene({ scene_id: "new-west", acquisition_date: "2026-03-05" }),
        satelliteScene({ scene_id: "new-east", acquisition_date: "2026-03-05", bbox: [114.5, 0.5, 115, 1] }),
        satelliteScene({ scene_id: "false", layer: FALSE_COLOR, acquisition_date: "2026-03-04" }),
      ]),
      hotspots: {
        items: [
          enrichedHotspot({ event_id: "a", observed_at: "2026-03-05T05:40:00Z" }),
          enrichedHotspot({ event_id: "b", observed_at: "2026-03-04T05:40:00Z" }),
        ],
        truncated: false,
      },
    });
    render(<IncidentReport incidentId={INCIDENT.incident_id} />);

    expect(await screen.findByRole("heading", { name: /Fire incident 0b6f3a52/ })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "← Back to map" })).toHaveAttribute("href", "#/");
    const details = within(screen.getByRole("region", { name: "Incident details" }));
    expect(details.getByText("42")).toBeInTheDocument();
    expect(details.getByText("High · 8/12")).toBeInTheDocument();
    expect(screen.getByRole("img", { name: /Growth over time: 3 days/ })).toBeInTheDocument();
    expect(within(screen.getByRole("region", { name: "Weather context" })).getByRole("table", { name: /Next 24 hours/ })).toBeInTheDocument();

    const map = await screen.findByLabelText("Satellite imagery map");
    expect(map).toHaveAttribute("data-scenes", "new-west,new-east");
    expect(map).toHaveAttribute("data-bounds", INCIDENT.bbox.join(","));
    await waitFor(() => expect(map).toHaveAttribute("data-detections", "1"));
    expect(screen.getByRole("radio", { name: "2026-03-05" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByText(/NOAA-20 VIIRS daily composite/)).toHaveTextContent("2 tiles");

    // The detections request covers the incident's box and days, duplicates included.
    const hotspotsUrl = fetchMock.mock.calls.map(([url]) => String(url)).find((url) => url.startsWith("/api/hotspots"));
    const params = new URL(String(hotspotsUrl), "http://localhost").searchParams;
    expect([params.get("west"), params.get("north")]).toEqual(["114.1", "0.55"]);
    expect([params.get("date_from"), params.get("date_to")]).toEqual(["2026-03-03", "2026-03-05"]);
    expect(params.get("include_duplicates")).toBe("true");

    fireEvent.click(screen.getByRole("radio", { name: "2026-03-04" }));
    expect(map).toHaveAttribute("data-scenes", "old");
    expect(map).toHaveAttribute("data-detections", "1");

    fireEvent.click(screen.getByRole("radio", { name: "False color" }));
    expect(map).toHaveAttribute("data-scenes", "false");
    expect(screen.getAllByRole("radio", { name: /^2026-/ })).toHaveLength(1);

    fireEvent.click(screen.getByLabelText(/Show detections from 2026-03-04/));
    expect(map).toHaveAttribute("data-detections", "0");
    fireEvent.change(screen.getByLabelText("Imagery opacity"), { target: { value: "0.4" } });
    expect(map).toHaveAttribute("data-opacity", "0.4");
  });

  it("defaults to the Sentinel-2 pass on a day with detections, with its time and colour key", async () => {
    stubApi({
      imagery: imagery([
        sentinelScene({ scene_id: "before", acquisition_date: "2026-03-01", cloud_cover: 3 }),
        sentinelScene({ scene_id: "during", acquisition_date: "2026-03-04", cloud_cover: 40 }),
        sentinelScene({ scene_id: "after", acquisition_date: "2026-03-08" }),
        satelliteScene({ scene_id: "daily", acquisition_date: "2026-03-05" }),
      ]),
    });
    render(<IncidentReport incidentId={INCIDENT.incident_id} />);

    const map = await screen.findByLabelText("Satellite imagery map");
    expect(map).toHaveAttribute("data-scenes", "during");
    expect(screen.getByRole("radio", { name: "Sentinel-2 SWIR (20 m)" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("radio", { name: "2026-03-04" })).toHaveClass("day-active");
    expect(screen.getByRole("radio", { name: "2026-03-01" })).toHaveAttribute("title", "Before the first detection");
    expect(screen.getByText(/Sentinel-2B MSI pass at/)).toHaveTextContent("2026-03-04 02:13 UTC · granule cloud cover 40–40%");
    expect(screen.getByRole("list", { name: "Colour key" })).toHaveTextContent("Fresh burn scar");

    fireEvent.click(screen.getByRole("radio", { name: "2026-03-08" }));
    expect(map).toHaveAttribute("data-scenes", "after");
    expect(screen.getByText("after the last detection")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("radio", { name: "True color" }));
    expect(screen.queryByRole("list", { name: "Colour key" })).not.toBeInTheDocument();
  });

  it("explains when no imagery has been acquired yet", async () => {
    stubApi({ imagery: imagery([]) });
    render(<IncidentReport incidentId={INCIDENT.incident_id} />);
    expect(await screen.findByText(/No satellite imagery has been acquired for this incident yet/)).toBeInTheDocument();
    expect(screen.queryByLabelText("Satellite imagery map")).not.toBeInTheDocument();
  });

  it("reports imagery failures without hiding the rest of the report", async () => {
    stubApi({ imagery: {}, status: 503 });
    render(<IncidentReport incidentId={INCIDENT.incident_id} />);
    expect(await screen.findByText("Could not load satellite imagery: Request failed (503).")).toBeInTheDocument();
    expect(screen.getByRole("img", { name: /Growth over time/ })).toBeInTheDocument();
  });

  it("reports an unknown incident", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 404, json: async () => ({}) }));
    render(<IncidentReport incidentId="missing" />);
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not load incident: Incident not found.");
  });
});
