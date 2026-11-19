import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Bounds, Hotspot } from "./hotspots";

vi.mock("./MapView", () => ({
  default: ({ hotspots, bounds, areaBounds, onSelect }: { hotspots: Hotspot[]; bounds: Bounds; areaBounds: Bounds | null; onSelect: (id: number) => void }) => (
    <div aria-label="Hotspot map" data-bounds={bounds.join(",")} data-area-bounds={areaBounds?.join(",") ?? ""}>
      {hotspots.map((hotspot) => (
        <button key={hotspot.id} onClick={() => onSelect(hotspot.id)}>Map point {hotspot.id}</button>
      ))}
    </div>
  ),
}));

const observation: Hotspot = {
  id: 7,
  source: "VIIRS_NOAA20_NRT",
  satellite: "N20",
  observed_at: "2026-03-05T00:07:00Z",
  latitude: 0.5234,
  longitude: 114.1234,
  attributes: { confidence: "nominal", frp: "12.5", daynight: "N" },
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("hotspot dashboard", () => {
  it("loads observations and shows details selected on the map", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);

    expect(await screen.findByText("0.523°, 114.123°")).toBeInTheDocument();
    const params = new URL(String(fetchMock.mock.calls[0][0]), "http://localhost").searchParams;
    expect(["west", "south", "east", "north"].some((key) => params.has(key))).toBe(false);
    expect(params.has("date_from")).toBe(true);
    expect(params.has("date_to")).toBe(true);
    expect(screen.getByLabelText("Geographic area")).toHaveValue("");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "92,-12,156,29");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "");
    expect(screen.queryByText("Dashed outline: area filter")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Map point 7" }));
    expect(screen.getByText("12.5 MW")).toBeInTheDocument();
    expect(screen.getByText("2026-03-05 00:07 UTC", { selector: "dd" })).toBeInTheDocument();
    expect(screen.getByText("nominal")).toBeInTheDocument();
  });

  it("applies geographic and date filters and flags truncated results", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: true }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByText("Result limit reached");
    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "sumatra" } });
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "");
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-03-01" } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-03-05" } });
    fireEvent.click(screen.getByRole("button", { name: /apply filters/i }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const url = String(fetchMock.mock.calls[1][0]);
    expect(url).toContain("west=94");
    expect(url).toContain("date_from=2026-03-01");
    expect(url).toContain("date_to=2026-03-05");
    expect(await screen.findByText(/Showing the most recent 5,000/)).toBeInTheDocument();
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "94,-7,106,6");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "94,-7,106,6");
    expect(screen.getByText("Dashed outline: area filter")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /apply filters/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    const resetParams = new URL(String(fetchMock.mock.calls[2][0]), "http://localhost").searchParams;
    expect(["west", "south", "east", "north"].some((key) => resetParams.has(key))).toBe(false);
    expect(resetParams.get("date_from")).toBe("2026-03-01");
    expect(resetParams.get("date_to")).toBe("2026-03-05");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", "92,-12,156,29");
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-area-bounds", "");
    expect(screen.queryByText("Dashed outline: area filter")).not.toBeInTheDocument();
  });

  it.each([
    ["philippines", "116,4,127,22"],
    ["newGuinea", "129,-12,156,1"],
  ])("applies the %s regional bounds", async (region, bounds) => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByText("0.523°, 114.123°");
    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: region } });
    fireEvent.click(screen.getByRole("button", { name: /apply filters/i }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const params = new URL(String(fetchMock.mock.calls[1][0]), "http://localhost").searchParams;
    expect(["west", "south", "east", "north"].map((key) => params.get(key)).join(",")).toBe(bounds);
    expect(screen.getByLabelText("Hotspot map")).toHaveAttribute("data-bounds", bounds);
  });

  it("reports failed requests and rejects an invalid date range", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    expect(await screen.findByText(/Could not load observations/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-03-06" } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-03-05" } });
    fireEvent.click(screen.getByRole("button", { name: /apply filters/i }));
    expect(screen.getByText(/Choose a valid date range/)).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("collapses panels and reopens observation details when a map point is selected", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [observation], truncated: false }),
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByText("0.523°, 114.123°");

    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "sumatra" } });
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

    const observations = screen.getByRole("button", { name: "Collapse observations" });
    fireEvent.click(observations);
    expect(observations).toHaveAttribute("aria-expanded", "false");
    expect(observations).toHaveAccessibleName("Expand observations");
    fireEvent.click(observations);
    expect(observations).toHaveAccessibleName("Collapse observations");
    fireEvent.click(observations);
    fireEvent.click(screen.getByRole("button", { name: "Map point 7" }));
    expect(observations).toHaveAttribute("aria-expanded", "true");
    expect(observations).toHaveAccessibleName("Collapse observations");
    expect(screen.getByText("12.5 MW")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /0.523°, 114.123°/ })).toHaveAttribute("aria-pressed", "true");

    fireEvent.click(screen.getByRole("button", { name: "Refresh data" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(screen.getByText(/Select a point on the map/)).toBeInTheDocument();
  });

  it("explains the scale and displays zero and unavailable FRP correctly", async () => {
    const zero = { ...observation, attributes: { frp: "0" } };
    const invalid = { ...observation, id: 8, attributes: { frp: "-1" } };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [zero, invalid], truncated: false }),
    }));
    render(<App />);
    expect(await screen.findByRole("group", { name: /FRP legend, logarithmic scale/ })).toHaveTextContent("≥100");
    expect(screen.getByText(/not fire severity or danger/)).toBeInTheDocument();
    fireEvent.click(await screen.findByRole("button", { name: "Map point 7" }));
    expect(screen.getByText("0 MW")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Map point 8" }));
    expect(screen.getByText("Fire radiative power").nextElementSibling).toHaveTextContent("Not provided");
  });
});
