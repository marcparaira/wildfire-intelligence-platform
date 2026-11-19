import { useEffect, useRef, useState } from "react";
import type { Hotspot } from "./hotspots";

export type LiveStatus = "connecting" | "live" | "reconnecting" | "unavailable";

const RECONNECT_MS = 5000;

/**
 * Subscribe to `GET /api/events/detections` and pass each batch of new detections
 * to `onDetections`.
 *
 * EventSource reconnects by itself after a dropped stream and resumes from its
 * Last-Event-ID. When the server answers with an error status instead (for example
 * a proxy 502 while the API restarts), EventSource gives up; this hook then opens
 * a new one after a delay and passes the last ID in the query string.
 */
export function useLiveDetections(
  query: string,
  onDetections: (detections: Hotspot[]) => void,
): LiveStatus {
  const [status, setStatus] = useState<LiveStatus>("connecting");
  const handler = useRef(onDetections);
  handler.current = onDetections;

  useEffect(() => {
    if (typeof EventSource === "undefined") {
      setStatus("unavailable");
      return;
    }
    let source: EventSource | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let lastEventId = "";

    function open() {
      const params = new URLSearchParams(query);
      if (lastEventId) params.set("last_event_id", lastEventId);
      const url = params.size ? `/api/events/detections?${params}` : "/api/events/detections";
      const current = new EventSource(url);
      source = current;
      current.addEventListener("ready", (event) => {
        lastEventId = event.lastEventId || lastEventId;
        setStatus("live");
      });
      current.addEventListener("detections", (event) => {
        lastEventId = event.lastEventId || lastEventId;
        setStatus("live");
        try {
          handler.current(JSON.parse(event.data as string) as Hotspot[]);
        } catch {
          // A malformed batch is skipped; the next one still applies.
        }
      });
      current.onerror = () => {
        setStatus("reconnecting");
        if (current.readyState === EventSource.CLOSED) {
          current.close();
          retry = setTimeout(open, RECONNECT_MS);
        }
      };
    }

    setStatus("connecting");
    open();
    return () => {
      clearTimeout(retry);
      source?.close();
    };
  }, [query]);

  return status;
}
