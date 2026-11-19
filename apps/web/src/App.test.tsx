import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Hotspot } from "./hotspots";

vi.mock("./MapView", () => ({
  default: ({ hotspots, onSelect }: { hotspots: Hotspot[]; onSelect: (id: number) => void }) => (
    <div aria-label="Hotspot map">
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
    expect(fetchMock.mock.calls[0][0]).toContain("west=108");
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
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-03-01" } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-03-05" } });
    fireEvent.click(screen.getByRole("button", { name: /apply filters/i }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const url = String(fetchMock.mock.calls[1][0]);
    expect(url).toContain("west=94");
    expect(url).toContain("date_from=2026-03-01");
    expect(url).toContain("date_to=2026-03-05");
    expect(await screen.findByText(/Showing the most recent 5,000/)).toBeInTheDocument();
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

    const filters = screen.getByRole("button", { name: "Filters" });
    fireEvent.click(filters);
    expect(filters).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByLabelText("Geographic area")).not.toBeVisible();
    fireEvent.click(filters);
    expect(screen.getByLabelText("Geographic area")).toBeInTheDocument();

    const observations = screen.getByRole("button", { name: "Observations" });
    fireEvent.click(observations);
    expect(observations).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(screen.getByRole("button", { name: "Map point 7" }));
    expect(observations).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("12.5 MW")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /0.523°, 114.123°/ })).toHaveAttribute("aria-pressed", "true");

    fireEvent.click(screen.getByRole("button", { name: "Refresh data" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(screen.getByText(/Select a point on the map/)).toBeInTheDocument();
  });
});
