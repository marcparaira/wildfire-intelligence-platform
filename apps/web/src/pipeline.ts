import { useEffect, useState } from "react";

export type Freshness = "fresh" | "delayed" | "stale" | "no_data" | "unknown";

export interface SourceStatus {
  id: string;
  label: string;
  kind: "observation" | "forecast";
  topic: string;
  freshness: Freshness;
  /** Newest observation time, or for a forecast the newest retrieval time. */
  latest_at: string | null;
  oldest_retrieved_at: string | null;
  forecast_until: string | null;
  latest_ingested_at: string | null;
  latest_stored_at: string | null;
  ingested_last_hour: number | null;
  ingested_last_day: number | null;
  stored: number | null;
  fresh_within_hours: number;
  stale_after_hours: number;
}

export interface TopicStatus {
  name: string;
  partitions: number;
  retained: number;
  messages_last_hour: number;
  messages_last_day: number;
  dead_letters_last_day: number;
}

export interface PartitionLag {
  partition: number;
  committed: number | null;
  end: number;
  lag: number;
}

export interface ConsumerStatus {
  group_id: string;
  topic: string;
  state: string;
  members: number;
  lag: number;
  partitions: PartitionLag[];
}

export interface DeadLetter {
  failed_at: string;
  topic: string | null;
  partition: number | null;
  offset: number | null;
  group_id: string | null;
  error: string;
}

export interface DeadLetterStatus {
  /** Every service's dead-letter topic. */
  topics: string[];
  retained: number;
  last_day: number;
  by_topic: Record<string, number>;
  sample_truncated: boolean;
  recent: DeadLetter[];
}

export interface PipelineStatus {
  checked_at: string;
  sources: SourceStatus[];
  topics: TopicStatus[] | null;
  consumers: ConsumerStatus[] | null;
  dead_letters: DeadLetterStatus | null;
  errors: string[];
}

export interface PipelineState {
  status: PipelineStatus | null;
  error: string;
  loading: boolean;
}

export const freshnessLabels: Record<Freshness, string> = {
  fresh: "Fresh",
  delayed: "Delayed",
  stale: "Stale",
  no_data: "No data",
  unknown: "Unknown",
};

function isPipelineStatus(value: unknown): value is PipelineStatus {
  return typeof value === "object" && value !== null && Array.isArray((value as PipelineStatus).sources);
}

/** Poll `GET /api/pipeline/status`; the last good status stays while a poll fails. */
export function usePipelineStatus(intervalMs: number): PipelineState {
  const [state, setState] = useState<PipelineState>({ status: null, error: "", loading: true });

  useEffect(() => {
    let controller: AbortController | null = null;

    function load() {
      controller?.abort();
      const current = new AbortController();
      controller = current;
      fetch("/api/pipeline/status", { signal: current.signal })
        .then((response) => {
          if (!response.ok) throw new Error(`Request failed (${response.status})`);
          return response.json() as Promise<unknown>;
        })
        .then((data) => {
          if (!isPipelineStatus(data)) throw new Error("Unexpected response");
          setState({ status: data, error: "", loading: false });
        })
        .catch((reason: unknown) => {
          if (current.signal.aborted) return;
          const message = reason instanceof Error ? reason.message : "Unable to load pipeline status";
          setState((previous) => ({ ...previous, error: message, loading: false }));
        });
    }

    load();
    const timer = setInterval(load, intervalMs);
    return () => {
      clearInterval(timer);
      controller?.abort();
    };
  }, [intervalMs]);

  return state;
}

/** "just now", "12 min ago", "3 h ago", "2 d ago"; future times read "in …". */
export function formatAge(timestamp: string | null, now: Date = new Date()): string {
  if (!timestamp) return "—";
  const time = Date.parse(timestamp);
  if (Number.isNaN(time)) return "—";
  const seconds = (now.getTime() - time) / 1000;
  const minutes = Math.round(Math.abs(seconds) / 60);
  let span: string;
  if (minutes < 1) return "just now";
  if (minutes < 60) span = `${minutes} min`;
  else if (minutes < 48 * 60) span = `${Math.round(minutes / 60)} h`;
  else span = `${Math.round(minutes / 1440)} d`;
  return seconds >= 0 ? `${span} ago` : `in ${span}`;
}

export function formatCount(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : value.toLocaleString("en-US");
}

/** "25 Nov" in UTC. */
export function formatDay(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
}
