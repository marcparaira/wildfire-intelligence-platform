import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { IncidentDetails, IncidentPanel } from "./IncidentPanel";
import { incidentDetail, incidentSummary } from "./test-fixtures";

afterEach(cleanup);

function panel(overrides: Partial<Parameters<typeof IncidentPanel>[0]> = {}) {
  const props = {
    items: [incidentSummary()],
    truncated: false,
    loading: false,
    error: "",
    selectedId: null,
    showOnMap: true,
    onShowOnMapChange: vi.fn(),
    onSelect: vi.fn(),
    ...overrides,
  };
  render(<IncidentPanel {...props} />);
  return props;
}

describe("incident panel", () => {
  it("lists incidents with status, size, activity, and risk", () => {
    const props = panel({
      items: [incidentSummary(), incidentSummary({ incident_id: "other", status: "new", peak_risk: null, footprint_km2: 5.2 })],
    });
    const list = within(screen.getByRole("region", { name: "Fire incidents" }));
    expect(list.getByText("2")).toBeInTheDocument();
    const rows = list.getAllByRole("button");
    expect(rows[0]).toHaveTextContent("Growing");
    expect(rows[0]).toHaveTextContent("High · 8/12");
    expect(rows[0]).toHaveTextContent("31 km²");
    expect(rows[0]).toHaveTextContent("42 detections");
    expect(rows[0]).toHaveTextContent("Last seen 2026-03-05 06:20 UTC");
    expect(rows[1]).toHaveTextContent("New");
    expect(rows[1]).toHaveTextContent("Risk unknown");
    expect(rows[1]).toHaveTextContent("5.2 km²");

    fireEvent.click(rows[1]);
    expect(props.onSelect).toHaveBeenCalledWith(expect.objectContaining({ incident_id: "other" }));
    fireEvent.click(screen.getByLabelText("Show incidents on map"));
    expect(props.onShowOnMapChange).toHaveBeenCalledWith(false);
  });

  it("marks the selected incident and explains empty, failed, and truncated lists", () => {
    panel({ selectedId: incidentSummary().incident_id, truncated: true });
    expect(screen.getByRole("button", { pressed: true })).toHaveTextContent("Growing");
    expect(screen.getByText("Showing the most recently active incidents only.")).toBeInTheDocument();
    cleanup();

    panel({ items: [] });
    expect(screen.getByText("No active incidents in this area.")).toBeInTheDocument();
    cleanup();

    panel({ items: [], loading: true });
    expect(screen.getByText("Loading incidents…")).toBeInTheDocument();
    cleanup();

    panel({ items: [], error: "Request failed (503)" });
    expect(screen.getByRole("alert")).toHaveTextContent("Could not load incidents: Request failed (503).");
  });
});

describe("incident details", () => {
  it("shows the lifecycle, growth chart, and weather context", () => {
    render(<IncidentDetails summary={incidentSummary()} incident={incidentDetail()} loading={false} error="" />);
    expect(screen.getByText("Growing (was new)")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View Full Report" })).toHaveAttribute(
      "href",
      `#/incidents/${incidentSummary().incident_id}`,
    );
    expect(screen.getByText("31 km² (6 cells)")).toBeInTheDocument();
    expect(screen.getByText("85.5 MW", { selector: "dd" })).toBeInTheDocument();
    expect(screen.getByText("VIIRS_NOAA20_NRT, MODIS_NRT")).toBeInTheDocument();

    const chart = screen.getByRole("img", { name: /Growth over time: 3 days from 2026-03-03 to 2026-03-05/ });
    expect(chart).toHaveAccessibleName(/up to 30 detections a day; footprint 31 km² at the end/);
    expect(chart.querySelectorAll("rect")).toHaveLength(3);
    const table = within(screen.getByRole("table", { name: "" }));
    expect(table.getByRole("rowheader", { name: "2026-03-04" }).closest("tr")).toHaveTextContent("2026-03-0430385.5 MW26 km²");

    expect(screen.getByText("Now (2026-03-05 07:00 UTC)")).toBeInTheDocument();
    expect(screen.getAllByText("14 km/h from NE").length).toBeGreaterThan(0);
    const outlook = within(screen.getByRole("table", { name: "Next 24 hours, every 3 hours" }));
    expect(outlook.getAllByRole("rowheader").map((cell) => cell.textContent)).toEqual([
      "10:00", "13:00", "16:00", "19:00", "22:00", "01:00", "04:00", "07:00",
    ]);
    expect(screen.getByText(/retrieved 2026-03-05 06:00 UTC/)).toBeInTheDocument();
  });

  it("shows the summary while the history loads, and degrades without a forecast", () => {
    render(<IncidentDetails summary={incidentSummary()} incident={null} loading error="" />);
    expect(screen.getByText("Loading history…")).toBeInTheDocument();
    expect(screen.getByText("High · 8/12")).toBeInTheDocument();
    cleanup();

    render(<IncidentDetails summary={null} incident={incidentDetail({ weather: null, status: "merged", previous_status: "growing", merged_into: "c0ffee00-1111-5222-8333-444455556666" })} loading={false} error="" />);
    expect(screen.getByText("No forecast is available for this incident's area.")).toBeInTheDocument();
    expect(screen.getByText("Merged into incident c0ffee00.")).toBeInTheDocument();
    cleanup();

    render(<IncidentDetails summary={null} incident={null} loading={false} error="Incident not found" />);
    expect(screen.getByRole("alert")).toHaveTextContent("Could not load incident: Incident not found.");
  });
});
