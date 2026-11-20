import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { formatAge, formatDay, usePipelineStatus } from "./pipeline";
import type { PipelineState, PipelineStatus } from "./pipeline";
import { PipelineStatusPage } from "./PipelineStatus";

const NOW = new Date("2026-03-05T12:00:00Z");

const status: PipelineStatus = {
  checked_at: "2026-03-05T11:59:50Z",
  sources: [
    {
      id: "MODIS_NRT", label: "MODIS Terra/Aqua", kind: "observation", topic: "firms.detections",
      freshness: "stale", latest_at: "2026-03-02T12:00:00Z", oldest_retrieved_at: null,
      forecast_until: null, latest_ingested_at: "2026-03-02T15:00:00Z",
      latest_stored_at: "2026-03-02T15:00:01Z", ingested_last_hour: 0, ingested_last_day: 0,
      stored: 812, fresh_within_hours: 18, stale_after_hours: 36,
    },
    {
      id: "open-meteo", label: "Open-Meteo forecast", kind: "forecast", topic: "weather.forecasts",
      freshness: "fresh", latest_at: "2026-03-05T10:00:00Z", oldest_retrieved_at: "2026-03-05T09:00:00Z",
      forecast_until: "2026-03-12T00:00:00Z", latest_ingested_at: "2026-03-05T10:00:00Z",
      latest_stored_at: "2026-03-05T10:00:02Z", ingested_last_hour: 0, ingested_last_day: 310,
      stored: 310, fresh_within_hours: 6, stale_after_hours: 24,
    },
  ],
  topics: [
    { name: "firms.detections", partitions: 6, retained: 51234, messages_last_hour: 12, messages_last_day: 1400, dead_letters_last_day: 2 },
  ],
  consumers: [
    {
      group_id: "sink-worker.postgis.detections", topic: "firms.detections", state: "Stable", members: 1, lag: 7,
      partitions: [{ partition: 0, committed: 10, end: 17, lag: 7 }, { partition: 1, committed: null, end: 0, lag: 0 }],
    },
  ],
  dead_letters: {
    topics: ["sink-worker.dlq", "stream-processor.dlq"], retained: 9, last_day: 2, by_topic: { "firms.detections": 2 }, sample_truncated: false,
    recent: [{
      failed_at: "2026-03-05T11:00:00Z", topic: "firms.detections", partition: 3, offset: 99,
      group_id: "sink-worker.postgis.detections", error: "h3_cell does not contain the detection",
    }],
  },
  errors: [],
};

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("formatAge", () => {
  it.each([
    [null, "—"],
    ["not a date", "—"],
    ["2026-03-05T11:59:40Z", "just now"],
    ["2026-03-05T11:48:00Z", "12 min ago"],
    ["2026-03-05T09:00:00Z", "3 h ago"],
    ["2026-03-03T12:00:00Z", "2 d ago"],
    ["2026-03-05T15:00:00Z", "in 3 h"],
  ])("formats %s as %s", (timestamp, expected) => {
    expect(formatAge(timestamp, NOW)).toBe(expected);
  });
});

it("formats a forecast day in UTC", () => {
  expect(formatDay("2026-03-12T23:00:00Z")).toBe("12 Mar");
});

function Probe({ onState }: { onState: (state: PipelineState) => void }) {
  onState(usePipelineStatus(15_000));
  return null;
}

describe("usePipelineStatus", () => {
  it("polls the status endpoint and keeps the last status when a poll fails", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => status })
      .mockResolvedValueOnce({ ok: false, status: 502 });
    vi.stubGlobal("fetch", fetchMock);
    let latest: PipelineState | null = null;
    render(<Probe onState={(state) => { latest = state; }} />);

    await waitFor(() => expect(latest?.status?.sources).toHaveLength(2));
    expect(String(fetchMock.mock.calls[0][0])).toBe("/api/pipeline/status");
    await vi.advanceTimersByTimeAsync(15_000);
    await waitFor(() => expect(latest?.error).toBe("Request failed (502)"));
    expect(latest!.status).toBe(status);
  });
});

describe("pipeline status page", () => {
  it("shows freshness, lag, throughput, and dead letters", () => {
    render(<PipelineStatusPage state={{ status, error: "", loading: false }} />);

    const sources = screen.getByRole("table", { name: "Sources" });
    expect(within(sources).getByRole("row", { name: /MODIS Terra\/Aqua/ })).toHaveTextContent("Stale");
    const weather = within(sources).getByRole("row", { name: /Open-Meteo forecast/ });
    expect(weather).toHaveTextContent("Fresh");
    expect(weather).toHaveTextContent("forecast until 2026-03-12 00:00 UTC");
    expect(weather).toHaveTextContent("310 cells");

    const lag = within(screen.getByRole("table", { name: "Consumer lag" })).getByRole("row", { name: /sink-worker/ });
    expect(lag).toHaveTextContent("Stable");
    expect(lag).toHaveTextContent("p0 7");
    expect(within(screen.getByRole("table", { name: "Topic throughput" })).getByRole("row", { name: /firms\.detections/ })).toHaveTextContent("1,400");
    expect(screen.getByText("h3_cell does not contain the detection")).toBeInTheDocument();
    expect(screen.getByText(/in the last 24 h, 9 retained/)).toHaveTextContent("sink-worker.dlq, stream-processor.dlq");
  });

  it("marks Kafka sections unavailable", () => {
    render(<PipelineStatusPage state={{
      status: { ...status, topics: null, consumers: null, dead_letters: null, errors: ["Kafka is unavailable"] },
      error: "",
      loading: false,
    }} />);
    expect(screen.getByText("Kafka is unavailable.")).toBeInTheDocument();
    expect(screen.getAllByText("Unavailable.")).toHaveLength(3);
  });
});
