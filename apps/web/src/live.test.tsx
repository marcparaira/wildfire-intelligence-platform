import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Hotspot } from "./hotspots";
import { mergeHotspots } from "./hotspots";

vi.mock("./MapView", () => ({
  default: ({ hotspots, onSelect }: { hotspots: Hotspot[]; onSelect: (eventId: string) => void }) => (
    <div aria-label="Hotspot map" data-count={hotspots.length}>
      {hotspots.map((hotspot) => (
        <button key={hotspot.event_id} onClick={() => onSelect(hotspot.event_id)}>Map point {hotspot.event_id}</button>
      ))}
    </div>
  ),
}));

class FakeEventSource {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;
  static instances: FakeEventSource[] = [];
  readonly url: string;
  readyState = FakeEventSource.CONNECTING;
  onerror: (() => void) | null = null;
  closed = false;
  private listeners = new Map<string, ((event: MessageEvent) => void)[]>();

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  close() {
    this.closed = true;
    this.readyState = FakeEventSource.CLOSED;
  }

  /** Like a browser, an event without data is not dispatched (it only moves the last ID). */
  emit(type: string, data: unknown, lastEventId: string) {
    this.readyState = FakeEventSource.OPEN;
    if (data === undefined) return;
    const event = new MessageEvent(type, { data: JSON.stringify(data), lastEventId });
    act(() => this.listeners.get(type)?.forEach((listener) => listener(event)));
  }

  fail() {
    this.readyState = FakeEventSource.CLOSED;
    act(() => this.onerror?.());
  }
}

function detection(eventId: string, observedAt: string, id: number | null = null): Hotspot {
  return {
    id,
    event_id: eventId,
    source: "VIIRS_NOAA20_NRT",
    instrument: "VIIRS",
    satellite: "N20",
    observed_at: observedAt,
    latitude: 0.5234,
    longitude: 114.1234,
    attributes: { frp: "12.5", confidence: "nominal", daynight: "D" },
    duplicate_of: null,
  };
}

const today = new Date().toISOString().slice(0, 10);
const stored = detection("stored", `${today}T00:07:00Z`, 7);

beforeEach(() => {
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
    ok: true,
    json: async () => ({ items: [stored], truncated: false }),
  }));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("live detections", () => {
  it("adds new detections to the map as they arrive, once each", async () => {
    render(<App />);
    expect(await screen.findByRole("button", { name: "Map point stored" })).toBeInTheDocument();
    const source = FakeEventSource.instances[0];
    expect(source.url).toBe("/api/events/detections");
    expect(screen.getByText("Connecting…")).toBeInTheDocument();

    source.emit("ready", {}, "0:10");
    expect(screen.getByText("Live")).toBeInTheDocument();

    const fresh = detection("fresh", `${today}T03:00:00Z`);
    source.emit("detections", [fresh, stored, { ...stored, id: null }], "0:13");
    source.emit("detections", [fresh], "0:14");

    const points = screen.getAllByRole("button", { name: /^Map point / });
    expect(points.map((point) => point.textContent)).toEqual(["Map point fresh", "Map point stored"]);
    expect(screen.getByText("New since load").querySelector("strong")).toHaveTextContent("1");

    fireEvent.click(screen.getByRole("button", { name: "Map point fresh" }));
    expect(screen.getByText("Live", { selector: ".detail-type span" })).toBeInTheDocument();
  });

  it("ignores live detections outside the selected dates", async () => {
    render(<App />);
    await screen.findByRole("button", { name: "Map point stored" });
    FakeEventSource.instances[0].emit("detections", [detection("old", "2001-01-01T00:00:00Z")], "0:1");
    expect(screen.queryByRole("button", { name: "Map point old" })).not.toBeInTheDocument();
  });

  it("streams only the selected area and restarts the stream when it changes", async () => {
    render(<App />);
    await screen.findByRole("button", { name: "Map point stored" });
    fireEvent.change(screen.getByLabelText("Geographic area"), { target: { value: "borneo" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));
    await screen.findByRole("button", { name: "Map point stored" });

    expect(FakeEventSource.instances).toHaveLength(2);
    expect(FakeEventSource.instances[0].closed).toBe(true);
    expect(FakeEventSource.instances[1].url).toBe("/api/events/detections?west=108&south=-5&east=120&north=8");
  });

  it("reopens a closed stream from the last event ID", async () => {
    render(<App />);
    await screen.findByRole("button", { name: "Map point stored" });
    vi.useFakeTimers();
    const first = FakeEventSource.instances[0];
    first.emit("detections", [detection("fresh", `${today}T03:00:00Z`)], "0:13");
    first.fail();
    expect(screen.getByText("Reconnecting…")).toBeInTheDocument();

    act(() => vi.advanceTimersByTime(5000));
    expect(FakeEventSource.instances[1].url).toBe("/api/events/detections?last_event_id=0%3A13");
  });
});

describe("mergeHotspots", () => {
  it("keeps the stored copy, sorts newest first, and trims to the limit", () => {
    const older = detection("older", "2026-03-05T01:00:00Z", 1);
    const newer = detection("newer", "2026-03-05T02:00:00Z");
    const newest = detection("newest", "2026-03-05T03:00:00Z");
    const merged = mergeHotspots([older], [newest, newer, { ...older, id: null }], 2);
    expect(merged.items.map((item) => item.event_id)).toEqual(["newest", "newer"]);
    expect(merged.trimmed).toBe(true);
    expect(mergeHotspots([older], []).items).toEqual([older]);
  });
});
