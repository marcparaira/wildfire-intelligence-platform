import { formatUtc } from "./hotspots";
import { formatAge, formatCount, formatDay, freshnessLabels } from "./pipeline";
import type { ConsumerStatus, PipelineState, SourceStatus } from "./pipeline";

function FreshnessBadge({ source }: { source: SourceStatus }) {
  const title = source.freshness === "unknown"
    ? "PostGIS could not be read"
    : `Fresh within ${source.fresh_within_hours} h, stale after ${source.stale_after_hours} h`;
  return <span className="freshness-badge" data-freshness={source.freshness} title={title}><i aria-hidden="true" />{freshnessLabels[source.freshness]}</span>;
}

function Timestamp({ value, now }: { value: string | null; now: Date }) {
  if (!value) return <>—</>;
  return <time dateTime={value} title={formatUtc(value)}>{formatAge(value, now)}</time>;
}

/** Compact per-source freshness for the dashboard sidebar. */
export function SourceFreshness({ state }: { state: PipelineState }) {
  const now = new Date();
  const { status, error, loading } = state;
  return (
    <section className="freshness" aria-label="Data freshness">
      <div className="panel-heading"><h2>Data freshness</h2><a href="#/status">Pipeline status</a></div>
      {!status && <p className="freshness-empty">{loading ? "Checking sources…" : `Freshness unavailable: ${error}.`}</p>}
      {status && (
        <ul>
          {status.sources.map((source) => (
            <li key={source.id}>
              <FreshnessBadge source={source} />
              <span className="freshness-source">{source.label}</span>
              <small>
                {source.kind === "forecast"
                  ? <>Retrieved <Timestamp value={source.latest_at} now={now} />{source.forecast_until && <>, covers to {formatDay(source.forecast_until)}</>}</>
                  : <>Observed <Timestamp value={source.latest_at} now={now} /></>}
              </small>
            </li>
          ))}
        </ul>
      )}
      {status && error && <p className="freshness-empty">Last check failed: {error}.</p>}
    </section>
  );
}

function PartitionList({ consumer }: { consumer: ConsumerStatus }) {
  return <span className="partition-list">{consumer.partitions.map((item) => <span key={item.partition} title={`Committed ${item.committed ?? "none"}, end ${item.end}`}>p{item.partition}&nbsp;{formatCount(item.lag)}</span>)}</span>;
}

export function PipelineStatusPage({ state }: { state: PipelineState }) {
  const now = new Date();
  const { status, error, loading } = state;
  const deadLettersByTopic = status?.dead_letters?.by_topic ?? {};

  return (
    <section className="status-page" aria-label="Pipeline status">
      <div className="status-header">
        <h2>Pipeline status</h2>
        {status && <span>Checked <Timestamp value={status.checked_at} now={now} /> · refreshes every 15 s</span>}
      </div>
      {loading && !status && <div className="map-message" role="status">Loading pipeline status…</div>}
      {error && <div className="map-message map-error" role="alert">Could not load pipeline status: {error}.</div>}
      {status?.errors.map((message) => <div key={message} className="notice" role="status">{message}.</div>)}

      {status && (
        <div className="status-sections">
          <section>
            <h3>Sources</h3>
            <p className="status-note">Freshness is the age of the newest observation, or of the newest forecast retrieval. Ingested counts use the time the ingestor fetched each record.</p>
            <div className="table-wrap">
              <table aria-label="Sources">
                <thead>
                  <tr><th>Source</th><th>Status</th><th>Newest data</th><th>Last ingested</th><th>Last stored</th><th className="num">Ingested 1 h</th><th className="num">Ingested 24 h</th><th className="num">In PostGIS</th><th className="num">Dead letters 24 h</th></tr>
                </thead>
                <tbody>
                  {status.sources.map((source) => (
                    <tr key={source.id}>
                      <th scope="row">{source.label}<small>{source.topic}</small></th>
                      <td><FreshnessBadge source={source} /></td>
                      <td>
                        <Timestamp value={source.latest_at} now={now} />
                        {source.kind === "forecast" && source.oldest_retrieved_at && <small>oldest cell <Timestamp value={source.oldest_retrieved_at} now={now} /></small>}
                        {source.forecast_until && <small>forecast until {formatUtc(source.forecast_until)}</small>}
                      </td>
                      <td><Timestamp value={source.latest_ingested_at} now={now} /></td>
                      <td><Timestamp value={source.latest_stored_at} now={now} /></td>
                      <td className="num">{formatCount(source.ingested_last_hour)}</td>
                      <td className="num">{formatCount(source.ingested_last_day)}</td>
                      <td className="num">{formatCount(source.stored)}{source.kind === "forecast" && source.stored !== null && " cells"}</td>
                      <td className="num">{status.dead_letters ? formatCount(deadLettersByTopic[source.topic] ?? 0) : "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section>
            <h3>Consumer lag</h3>
            <p className="status-note">Records each sink-worker consumer group has yet to project into PostGIS.</p>
            {status.consumers ? (
              <div className="table-wrap">
                <table aria-label="Consumer lag">
                  <thead><tr><th>Consumer group</th><th>Topic</th><th>State</th><th className="num">Members</th><th className="num">Lag</th><th>Per partition</th></tr></thead>
                  <tbody>
                    {status.consumers.map((consumer) => (
                      <tr key={consumer.group_id}>
                        <th scope="row">{consumer.group_id}</th>
                        <td>{consumer.topic}</td>
                        <td><span className="group-state" data-state={consumer.state}>{consumer.state}</span></td>
                        <td className="num">{consumer.members}</td>
                        <td className="num"><strong>{formatCount(consumer.lag)}</strong></td>
                        <td><PartitionList consumer={consumer} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : <p className="status-unavailable">Unavailable.</p>}
          </section>

          <section>
            <h3>Topic throughput</h3>
            {status.topics ? (
              <div className="table-wrap">
                <table aria-label="Topic throughput">
                  <thead><tr><th>Topic</th><th className="num">Partitions</th><th className="num">Messages 1 h</th><th className="num">Messages 24 h</th><th className="num">Retained offsets</th><th className="num">Dead letters 24 h</th></tr></thead>
                  <tbody>
                    {status.topics.map((topic) => (
                      <tr key={topic.name}>
                        <th scope="row">{topic.name}</th>
                        <td className="num">{topic.partitions}</td>
                        <td className="num">{formatCount(topic.messages_last_hour)}</td>
                        <td className="num">{formatCount(topic.messages_last_day)}</td>
                        <td className="num">{formatCount(topic.retained)}</td>
                        <td className="num">{formatCount(topic.dead_letters_last_day)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : <p className="status-unavailable">Unavailable.</p>}
            <p className="status-note">Retained offsets of a compacted topic overstate the records it still holds.</p>
          </section>

          <section>
            <h3>Dead letters</h3>
            {status.dead_letters ? (
              <>
                <p className="status-note">
                  <strong>{formatCount(status.dead_letters.last_day)}</strong> in the last 24 h, {formatCount(status.dead_letters.retained)} retained in <code>{status.dead_letters.topic}</code>.
                  {status.dead_letters.sample_truncated && " Per-topic counts cover only the newest records."}
                </p>
                {status.dead_letters.recent.length > 0 ? (
                  <div className="table-wrap">
                    <table aria-label="Recent dead letters">
                      <thead><tr><th>Failed</th><th>Original record</th><th>Consumer group</th><th>Error</th></tr></thead>
                      <tbody>
                        {status.dead_letters.recent.map((letter) => (
                          <tr key={`${letter.failed_at}-${letter.topic}-${letter.partition}-${letter.offset}`}>
                            <td><Timestamp value={letter.failed_at} now={now} /></td>
                            <td>{letter.topic ?? "unknown"}{letter.partition !== null && `[${letter.partition}]`}{letter.offset !== null && `@${letter.offset}`}</td>
                            <td>{letter.group_id ?? "—"}</td>
                            <td className="error-cell">{letter.error || "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : <p className="status-unavailable">No dead letters in the last 24 hours.</p>}
              </>
            ) : <p className="status-unavailable">Unavailable.</p>}
          </section>
        </div>
      )}
    </section>
  );
}
