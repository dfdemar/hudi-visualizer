import React, { useState, useEffect, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip } from "recharts";

/**
 * Hudi Processing Visualizer (enhanced)
 * - Adds realistic example datasets (NYC Taxi, Retail Orders, GitHub Events)
 * - Adds storage types: Merge-on-Read (MoR) vs Copy-on-Write (CoW)
 * - Adds a Timeline with Hudi instants (REQUESTED/INFLIGHT/COMPLETED) for commit, deltacommit, compaction, and clean
 * - Read simulator supports Snapshot/Incremental and time-travel "as of instant"
 * - Single-file React component (Tailwind + recharts + framer-motion)
 */

/***********************************\
|* Helpers & Data Generators       *|
\***********************************/

const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const pad2 = (n) => (n < 10 ? `0${n}` : `${n}`);
const dateStr = (d) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
const timeStr = (d) => `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;

const makeInstantTime = () => {
  // Hudi instants are often timestamp-based (yyyyMMddHHmmssSSS)
  const d = new Date();
  const SSS = `${d.getMilliseconds()}`.padStart(3, "0");
  return (
    `${d.getFullYear()}` +
    pad2(d.getMonth() + 1) +
    pad2(d.getDate()) +
    pad2(d.getHours()) +
    pad2(d.getMinutes()) +
    pad2(d.getSeconds()) +
    SSS
  );
};

// Convert Hudi instantTime (yyyyMMddHHmmssSSS) to a Date
const instantToDate = (instant) => {
  if (!instant) return null;
  try {
    const yyyy = parseInt(instant.slice(0, 4), 10);
    const MM = parseInt(instant.slice(4, 6), 10) - 1;
    const dd = parseInt(instant.slice(6, 8), 10);
    const hh = parseInt(instant.slice(8, 10), 10);
    const mm = parseInt(instant.slice(10, 12), 10);
    const ss = parseInt(instant.slice(12, 14), 10);
    const SSS = parseInt(instant.slice(14, 17) || "0", 10);
    return new Date(yyyy, MM, dd, hh, mm, ss, SSS);
  } catch (e) {
    return null;
  }
};

// Example datasets
const DATASETS = {
  nycTaxi: {
    name: "NYC Taxi Trips",
    key: "trip_id",
    partitionBy: (r) => `dt=${r.pickup_date}`,
    generate: (n) => {
      const rows = [];
      for (let i = 0; i < n; i++) {
        const d = new Date(Date.now() - rand(0, 7) * 24 * 3600 * 1000);
        rows.push({
          trip_id: `t${Date.now()}_${i}_${rand(1000, 9999)}`,
          pickup_datetime: `${dateStr(d)} ${timeStr(d)}`,
          pickup_date: dateStr(d),
          passenger_count: rand(1, 5),
          total_amount: Number((Math.random() * 80 + 3).toFixed(2)),
          vendor_id: ["CMT", "VTS"][rand(0, 1)],
        });
      }
      return rows;
    },
  },
  retail: {
    name: "Retail Orders",
    key: "order_id",
    partitionBy: (r) => `region=${r.region}`,
    generate: (n) => {
      const regions = ["US-EAST", "US-WEST", "EU", "APAC"];
      const status = ["PENDING", "PAID", "SHIPPED", "CANCELLED"];
      const rows = [];
      for (let i = 0; i < n; i++) {
        rows.push({
          order_id: `o${Date.now()}_${i}_${rand(1000, 9999)}`,
          customer_id: `c${rand(1, 5000)}`,
          order_ts: new Date().toISOString(),
          amount: Number((Math.random() * 500 + 5).toFixed(2)),
          status: status[rand(0, status.length - 1)],
          region: regions[rand(0, regions.length - 1)],
        });
      }
      return rows;
    },
  },
  ghEvents: {
    name: "GitHub Events",
    key: "event_id",
    partitionBy: (r) => `dt=${r.date}`,
    generate: (n) => {
      const types = ["PushEvent", "PullRequestEvent", "IssueCommentEvent", "WatchEvent"];
      const repos = ["apache/hudi", "vercel/next.js", "facebook/react", "pallets/flask", "numpy/numpy"];
      const rows = [];
      for (let i = 0; i < n; i++) {
        const d = new Date(Date.now() - rand(0, 3) * 24 * 3600 * 1000);
        rows.push({
          event_id: `e${Date.now()}_${i}_${rand(1000, 9999)}`,
          repo: repos[rand(0, repos.length - 1)],
          type: types[rand(0, types.length - 1)],
          actor: `user${rand(1, 2000)}`,
          date: dateStr(d),
          created_at: new Date().toISOString(),
        });
      }
      return rows;
    },
  },
};

// FileGroup structure (simplified): { id, partition, baseFiles: [{id, version, rows, ts, instantTime}], deltaFiles: [{id, version, rows, ts, instantTime}] }
let fileGroupCounter = 1;
const makeFileGroup = (partition = "default") => ({
  id: `fg-${fileGroupCounter++}`,
  partition,
  baseFiles: [],
  deltaFiles: [],
});

/***********************************\
|* Top-level Component             *|
\***********************************/
export default function HudiVisualizer() {
  // simulation state
  const [datasetKey, setDatasetKey] = useState("nycTaxi");
  const dataset = DATASETS[datasetKey];

  const [storage, setStorage] = useState("mor"); // 'mor' | 'cow'
  const [fileGroups, setFileGroups] = useState([makeFileGroup("p0"), makeFileGroup("p1")]);
  const [ingestBuffer, setIngestBuffer] = useState([]);
  const [autoIngest, setAutoIngest] = useState(true);
  const [ingestRate, setIngestRate] = useState(2); // records per tick

  // Hudi timeline (instants)
  // Each instant: { instantTime, type: 'commit'|'deltacommit'|'compaction'|'clean', state: 'REQUESTED'|'INFLIGHT'|'COMPLETED', records, notes, writtenRecords? }
  const [timeline, setTimeline] = useState([]);
  const [selectedInstant, setSelectedInstant] = useState(null);

  // heartbeat
  useEffect(() => {
    const id = setInterval(() => {
      if (autoIngest) {
        bufferGenerate(ingestRate);
      }
    }, 1000);
    return () => clearInterval(id);
  }, [autoIngest, ingestRate, datasetKey]);

  const bufferGenerate = (count) => {
    setIngestBuffer((buf) => [...buf, ...dataset.generate(count)]);
  };

  // Utility to push/update an instant
  const pushInstant = (patch) => {
    setTimeline((t) => [{ ...patch }, ...t]); // newest at top for UI convenience
  };
  const updateInstant = (instantTime, patch) => {
    setTimeline((t) => t.map((i) => (i.instantTime === instantTime ? { ...i, ...patch } : i)));
  };

  // Commit buffered records
  function commitBufferedRecords({ op = "upsert" } = {}) {
    if (ingestBuffer.length === 0) return;
    const instantTime = makeInstantTime();

    // snapshot buffer for this commit so we can inspect later
    const writtenRecords = [...ingestBuffer];

    // REQUESTED
    pushInstant({ instantTime, type: storage === "mor" ? "deltacommit" : "commit", state: "REQUESTED", records: ingestBuffer.length, notes: op, writtenRecords });

    // INFLIGHT
    updateInstant(instantTime, { state: "INFLIGHT" });

    // Apply write
    setFileGroups((prev) => {
      const groups = prev.map((g) => ({ ...g }));
      let assigned = 0;
      const records = [...ingestBuffer];
      let remaining = records.length;
      while (remaining > 0) {
        const r = records[records.length - remaining];
        const part = dataset.partitionBy(r);
        // choose/ensure a file group for that partition (round-robin per partition)
        let target = groups[assigned % groups.length];
        if (!target || target.partition !== part) {
          // find a group with this partition or create one
          target = groups.find((g) => g.partition === part) || makeFileGroup(part);
          if (!groups.includes(target)) groups.push(target);
        }
        if (storage === "mor") {
          // Write to delta (log) file
          const df = {
            id: `delta-${target.id}-${instantTime}-${Math.floor(Math.random() * 1000)}`,
            version: target.deltaFiles.length + 1,
            rows: 1,
            ts: Date.now(),
            instantTime,
          };
          target.deltaFiles.push(df);
        } else {
          // CoW: rewrite base (simulate creating a new base file version)
          const rowsInThis = 1;
          const newBase = {
            id: `base-${target.id}-${instantTime}-${Math.floor(Math.random() * 1000)}`,
            version: target.baseFiles.length + 1,
            rows: (target.baseFiles.at(-1)?.rows || 0) + rowsInThis,
            ts: Date.now(),
            instantTime,
          };
          target.baseFiles.push(newBase);
          // No deltas in CoW path
          target.deltaFiles = [];
        }
        assigned++;
        remaining--;
      }
      return groups;
    });

    // Clear buffer
    setIngestBuffer([]);
    // COMPLETED
    updateInstant(instantTime, { state: "COMPLETED" });
  }

  // Compaction (MoR only): can be scheduled, then run later
  function scheduleCompaction() {
    if (storage !== "mor") return;
    const instantTime = makeInstantTime();
    pushInstant({ instantTime, type: "compaction", state: "REQUESTED", records: 0, notes: "scheduled", writtenRecords: [] });
  }
  function runScheduledCompactions() {
    if (storage !== "mor") return;
    // Find REQUESTED compactions and execute
    setTimeline((t) => {
      const toRun = t.filter((i) => i.type === "compaction" && i.state === "REQUESTED");
      if (toRun.length === 0) return t;
      toRun.forEach((inst) => {
        updateInstant(inst.instantTime, { state: "INFLIGHT" });
      });
      // Merge deltas to base
      setFileGroups((prev) =>
        prev.map((fg) => {
          if (fg.deltaFiles.length === 0) return fg;
          const totalDeltaRows = fg.deltaFiles.reduce((s, d) => s + d.rows, 0);
          const newBase = {
            id: `base-${fg.id}-${makeInstantTime()}`,
            version: fg.baseFiles.length + 1,
            rows: (fg.baseFiles.at(-1)?.rows || 0) + totalDeltaRows,
            ts: Date.now(),
            instantTime: makeInstantTime(),
          };
          return { ...fg, baseFiles: [...fg.baseFiles, newBase], deltaFiles: [] };
        })
      );
      toRun.forEach((inst) => updateInstant(inst.instantTime, { state: "COMPLETED" }));
      return t;
    });
  }

  // Clean: keep only last N base versions per file group
  function cleanOlderVersions(keep = 1) {
    const instantTime = makeInstantTime();
    pushInstant({ instantTime, type: "clean", state: "REQUESTED", records: 0, writtenRecords: [] });
    updateInstant(instantTime, { state: "INFLIGHT" });

    setFileGroups((prev) =>
      prev.map((fg) => {
        if (fg.baseFiles.length <= keep) return fg;
        const sorted = [...fg.baseFiles].sort((a, b) => b.ts - a.ts);
        return { ...fg, baseFiles: sorted.slice(0, keep) };
      })
    );

    updateInstant(instantTime, { state: "COMPLETED" });
  }

  function addPartition() {
    setFileGroups((g) => [...g, makeFileGroup(`p${g.length}`)]);
  }

  const totalRows = useMemo(
    () =>
      fileGroups.reduce(
        (s, fg) =>
          s +
          fg.baseFiles.reduce((sb, b) => sb + b.rows, 0) +
          fg.deltaFiles.reduce((sd, d) => sd + d.rows, 0),
        0
      ),
    [fileGroups]
  );

  // Build timeline data for charts
  const timelineChartData = useMemo(() => {
    const items = [...timeline].reverse(); // oldest -> newest
    return items.map((i, idx) => ({
      idx: idx + 1,
      records: i.records || 0,
      type: i.type,
      state: i.state,
      instantTime: i.instantTime,
    }));
  }, [timeline]);

  return (
    <div className="p-6 min-h-screen bg-slate-50 text-slate-900">
      <div className="max-w-6xl mx-auto">
        <header className="mb-6">
          <h1 className="text-3xl font-extrabold">Hudi Processing Visualizer</h1>
          <p className="text-sm text-slate-600">Realistic datasets, Merge-on-Read vs Copy-on-Write, and a true Hudi timeline with instants.</p>
        </header>

        <main className="grid grid-cols-12 gap-6">
          {/* Controls */}
          <section className="col-span-4 bg-white p-4 rounded-2xl shadow space-y-4">
            <div>
              <h2 className="font-semibold mb-2">Dataset</h2>
              <div className="flex items-center gap-2">
                <select value={datasetKey} onChange={(e) => setDatasetKey(e.target.value)} className="text-sm p-2 rounded border w-full">
                  {Object.entries(DATASETS).map(([k, v]) => (
                    <option key={k} value={k}>{v.name}</option>
                  ))}
                </select>
              </div>
              <div className="flex items-center gap-2 mt-3">
                <button className="px-3 py-2 rounded bg-slate-800 text-white text-sm" onClick={() => bufferGenerate(25)}>Load 25 to Buffer</button>
                <button className="px-3 py-2 rounded bg-slate-200 text-slate-900 text-sm" onClick={() => setIngestBuffer([])}>Clear Buffer</button>
              </div>
              <div className="mt-2 text-xs text-slate-500">Partitioning varies by dataset (date vs region). Keys: <span className="font-mono">{dataset.key}</span></div>
            </div>

            <div>
              <h2 className="font-semibold mb-2">Write Storage</h2>
              <div className="flex gap-2">
                <button className={`px-3 py-2 rounded text-sm ${storage === 'mor' ? 'bg-sky-600 text-white' : 'bg-slate-100'}`} onClick={() => setStorage('mor')}>Merge-on-Read</button>
                <button className={`px-3 py-2 rounded text-sm ${storage === 'cow' ? 'bg-sky-600 text-white' : 'bg-slate-100'}`} onClick={() => setStorage('cow')}>Copy-on-Write</button>
              </div>
              <p className="text-xs text-slate-500 mt-2">
                MoR writes small delta (log) files and later compacts to base files. CoW rewrites base files directly on each commit.
              </p>
            </div>

            <div>
              <h2 className="font-semibold mb-2">Ingest</h2>
              <div className="mb-2 flex items-center gap-3">
                <label className="block text-xs text-slate-500">Auto ingest</label>
                <button className={`px-3 py-1 rounded ${autoIngest ? 'bg-sky-600 text-white' : 'bg-slate-100'}`} onClick={() => setAutoIngest((v) => !v)}>{autoIngest ? 'On' : 'Off'}</button>
                <div className="text-xs text-slate-500">Records/sec: {ingestRate}</div>
              </div>
              <input type="range" min={0} max={5} value={ingestRate} onChange={(e) => setIngestRate(Number(e.target.value))} className="w-full" />

              <div className="mt-3 text-sm">Buffer: <span className="font-semibold">{ingestBuffer.length}</span> records</div>
              <div className="flex gap-2 mt-2">
                <button className="px-3 py-2 rounded bg-emerald-500 text-white text-sm" onClick={() => commitBufferedRecords({ op: 'upsert' })}>Commit (Upsert)</button>
                <button className="px-3 py-2 rounded bg-indigo-500 text-white text-sm" onClick={() => commitBufferedRecords({ op: 'insert' })}>Commit (Insert)</button>
              </div>
            </div>

            <div>
              <h2 className="font-semibold mb-2">Table Services</h2>
              <div className="flex gap-2">
                <button className="px-3 py-2 rounded bg-orange-400 text-white text-sm" onClick={addPartition}>Add Partition</button>
                <button className="px-3 py-2 rounded bg-rose-500 text-white text-sm" onClick={() => cleanOlderVersions(1)}>Clean (keep 1)</button>
              </div>
              <div className="flex gap-2 mt-2">
                <button className={`px-3 py-2 rounded text-sm ${storage !== 'mor' ? 'opacity-50 cursor-not-allowed' : 'bg-yellow-500 text-white'}`} disabled={storage !== 'mor'} onClick={scheduleCompaction}>Schedule Compaction</button>
                <button className={`px-3 py-2 rounded text-sm ${storage !== 'mor' ? 'opacity-50 cursor-not-allowed' : 'bg-yellow-600 text-white'}`} disabled={storage !== 'mor'} onClick={runScheduledCompactions}>Run Scheduled</button>
              </div>
              <p className="text-xs text-slate-500 mt-2">Compaction is only applicable for Merge-on-Read.</p>
            </div>

            <div className="mt-2 text-xs text-slate-500">Total rows (base + delta): {totalRows}</div>
          </section>

          {/* Visualization */}
          <section className="col-span-8 space-y-4">
            {/* FileGroups */}
            <div className="bg-white p-4 rounded-2xl shadow">
              <div className="flex justify-between items-center">
                <h2 className="font-semibold">Partition FileGroups</h2>
                <div className="text-sm text-slate-500">MoR shows deltas accumulating; CoW rewrites base versions.</div>
              </div>

              <div className="mt-4 grid grid-cols-1 gap-3">
                {fileGroups.map((fg) => (
                  <motion.div key={fg.id} layout className="p-3 border rounded-lg">
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <div className="text-sm font-medium">{fg.id} — {fg.partition}</div>
                        <div className="text-xs text-slate-500">Base: {fg.baseFiles.length} • Delta: {fg.deltaFiles.length}</div>
                        <div className="flex flex-wrap gap-2 mt-2">
                          {fg.baseFiles.map((b) => (
                            <span key={b.id} className="px-2 py-1 text-xs bg-slate-100 rounded">B v{b.version} ({b.rows}r)</span>
                          ))}
                          {fg.deltaFiles.map((d) => (
                            <span key={d.id} className="px-2 py-1 text-xs bg-amber-100 rounded">Δ v{d.version} ({d.rows}r)</span>
                          ))}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-xs text-slate-500">Last base: {fg.baseFiles.at(-1)?.instantTime || '—'}</div>
                        <div className="text-xs text-slate-500">Last delta: {fg.deltaFiles.at(-1)?.instantTime || '—'}</div>
                      </div>
                    </div>
                  </motion.div>
                ))}
              </div>
            </div>

            {/* Hudi Timeline */}
            <div className="bg-white p-4 rounded-2xl shadow">
              <div className="flex items-center justify-between">
                <h2 className="font-semibold">Timeline (Instants)</h2>
                <span className="text-xs text-slate-500">Types: commit / deltacommit / compaction / clean</span>
              </div>

              {/* Bars instead of a line */}
              <div style={{ height: 180 }} className="mt-3">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={timelineChartData}>
                    <XAxis dataKey="idx" />
                    <YAxis />
                    <Tooltip
                      formatter={(value, name) => {
                        if (name === 'records') return [`${value} records`, 'records'];
                        return [value, name];
                      }}
                      labelFormatter={(label, payload) => {
                        const p = (payload && payload[0] && payload[0].payload) || {};
                        return `${p.type || ''} #${label}`;
                      }}
                    />
                    <Bar dataKey="records" />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div className="mt-3 grid grid-cols-1 gap-2">
                <AnimatePresence>
                  {timeline.slice(0, 8).map((ins) => {
                    const d = instantToDate(ins.instantTime);
                    return (
                      <motion.div
                        key={ins.instantTime}
                        initial={{ opacity: 0, y: -4 }}
                        animate={{ opacity: 1, y: 0 }}
                        exit={{ opacity: 0 }}
                        className="px-3 py-2 bg-slate-50 rounded flex items-center justify-between hover:bg-slate-100 cursor-pointer"
                        onClick={() => setSelectedInstant(ins)}
                        title="Click to view records written"
                      >
                        <div>
                          <div className="text-sm font-medium">
                            {ins.type}
                            <span
                              className={`ml-2 text-[11px] px-2 py-0.5 rounded-full ${
                                ins.state === 'COMPLETED'
                                  ? 'bg-emerald-100 text-emerald-700'
                                  : ins.state === 'INFLIGHT'
                                  ? 'bg-yellow-100 text-yellow-700'
                                  : 'bg-slate-200 text-slate-700'
                              }`}
                            >
                              {ins.state}
                            </span>
                          </div>
                          <div className="text-xs text-slate-500">
                            instant: {ins.instantTime} • records: {ins.records || 0} {ins.notes ? `• ${ins.notes}` : ''}
                          </div>
                        </div>
                        <div className="text-xs text-slate-400">
                          {d ? `${d.toLocaleDateString()} ${d.toLocaleTimeString()}` : '—'}
                        </div>
                      </motion.div>
                    );
                  })}
                </AnimatePresence>
              </div>

              {/* Commit detail: records written */}
              {selectedInstant && (
                <div className="mt-4 border rounded-xl p-3 bg-slate-50">
                  <div className="flex items-center justify-between mb-2">
                    <div className="text-sm font-semibold">
                      {selectedInstant.type}@{selectedInstant.instantTime}
                    </div>
                    <button
                      className="text-xs px-2 py-1 rounded bg-slate-200"
                      onClick={() => setSelectedInstant(null)}
                    >
                      Close
                    </button>
                  </div>
                  <div className="text-xs text-slate-600 mb-2">
                    {selectedInstant.records || 0} records written{selectedInstant.notes ? ` • ${selectedInstant.notes}` : ''}
                  </div>
                  {Array.isArray(selectedInstant.writtenRecords) && selectedInstant.writtenRecords.length > 0 ? (
                    <div className="max-h-64 overflow-auto text-xs">
                      <table className="w-full text-left">
                        <thead className="sticky top-0 bg-slate-100">
                          <tr>
                            <th className="px-2 py-1">#</th>
                            <th className="px-2 py-1">Record (JSON)</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedInstant.writtenRecords.slice(0, 200).map((r, idx) => (
                            <tr key={idx} className="odd:bg-white even:bg-slate-50">
                              <td className="px-2 py-1 align-top">{idx + 1}</td>
                              <td className="px-2 py-1 font-mono align-top whitespace-pre-wrap">{JSON.stringify(r, null, 2)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {selectedInstant.writtenRecords.length > 200 && (
                        <div className="text-[11px] text-slate-500 mt-2">Showing first 200 of {selectedInstant.writtenRecords.length} records.</div>
                      )}
                    </div>
                  ) : (
                    <div className="text-xs text-slate-500">No per-record payload for this instant.</div>
                  )}
                </div>
              )}
            </div>

            {/* Read Simulator */}
            <div className="bg-white p-4 rounded-2xl shadow">
              <h2 className="font-semibold">Read Models / Queries</h2>
              <p className="text-xs text-slate-500 mt-2">Compare Snapshot vs Incremental, and time travel to a specific instant.</p>

              <ReadSimulator fileGroups={fileGroups} timeline={timeline} storage={storage} />
            </div>
          </section>
        </main>

        <footer className="mt-6 text-xs text-slate-500">Educational visualization of Apache Hudi concepts. Not a substitute for production behavior.</footer>
      </div>
    </div>
  );
}

/***********************************\
|* Read Simulator                  *|
\***********************************/
function ReadSimulator({ fileGroups, timeline, storage }) {
  const [mode, setMode] = useState("snapshot"); // 'snapshot' | 'incremental'
  const completedInstants = useMemo(() => timeline.filter((i) => i.state === "COMPLETED"), [timeline]);
  const [asOf, setAsOf] = useState(completedInstants[0]?.instantTime || null);

  useEffect(() => {
    if (!completedInstants.length) return;
    setAsOf(completedInstants[0].instantTime);
  }, [completedInstants.length]);

  // Compute rows for current read, optionally as-of an instant
  const { snapshotRows, incrementalRows } = useMemo(() => {
    // For simplicity, treat each base/delta file as 1-row chunk entries with accumulated counts in base rows
    // Time travel: include only files whose instantTime <= asOf
    const includeFile = (f) => (asOf ? f.instantTime <= asOf : true);

    let baseTotal = 0;
    let deltaTotal = 0;
    fileGroups.forEach((fg) => {
      fg.baseFiles.filter(includeFile).forEach((b) => (baseTotal = Math.max(baseTotal, b.rows))); // take latest base rows count
      fg.deltaFiles.filter(includeFile).forEach((d) => (deltaTotal += d.rows));
    });

    // Snapshot reads:
    // - MoR: base + merged deltas (as-of)
    // - CoW: just the latest base (as-of)
    const snapshot = storage === "mor" ? baseTotal + deltaTotal : baseTotal;

    // Incremental reads: only changes since last as-of instant (approx here: count deltas at that instant)
    const inc = fileGroups.reduce(
      (s, fg) => s + fg.deltaFiles.filter((d) => includeFile(d) && (!asOf || d.instantTime === asOf)).reduce((sd, d) => sd + d.rows, 0),
      0
    );

    return { snapshotRows: snapshot, incrementalRows: inc };
  }, [fileGroups, asOf, storage]);

  return (
    <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="p-3 bg-slate-50 rounded">
        <div className="flex items-center gap-2">
          <label className="text-sm font-medium">Read Mode</label>
          <select value={mode} onChange={(e) => setMode(e.target.value)} className="ml-auto text-sm p-1 rounded border">
            <option value="snapshot">Snapshot (latest/as-of)</option>
            <option value="incremental">Incremental (as-of)</option>
          </select>
        </div>

        <div className="mt-3">
          <div className="text-xs text-slate-500 mb-1">As of instant</div>
          <select value={asOf || ''} onChange={(e) => setAsOf(e.target.value || null)} className="text-sm p-1 rounded border w-full">
            <option value="">(latest)</option>
            {completedInstants.map((i) => (
              <option key={i.instantTime} value={i.instantTime}>{i.type}@{i.instantTime}</option>
            ))}
          </select>
        </div>

        <div className="mt-3 text-sm">Rows returned: <span className="font-semibold">{mode === 'snapshot' ? snapshotRows : incrementalRows}</span></div>
        <div className="text-xs text-slate-500 mt-1">Storage: {storage.toUpperCase()} • {mode === 'snapshot' ? 'Base + (MoR)Deltas / (CoW)Base' : 'Deltas at instant'}</div>
      </div>

      <div className="p-3 bg-slate-50 rounded">
        <div className="text-sm font-medium">Explain</div>
        <div className="text-xs text-slate-600 mt-2">
          <ul className="list-disc list-inside space-y-1">
            <li><b>Merge-on-Read</b>: Writes land in <i>delta</i> (log) files via <code>deltacommit</code>. Compaction later creates new base files.</li>
            <li><b>Copy-on-Write</b>: Each commit rewrites <i>base</i> files; no deltas or compaction.</li>
            <li><b>Timeline (instants)</b>: Operations move through <code>REQUESTED → INFLIGHT → COMPLETED</code>.</li>
            <li><b>Snapshot reads</b>: MoR = base merged with deltas; CoW = latest base only. Time travel supported via "as of instant".</li>
            <li><b>Incremental reads</b>: Return changes at or since a given instant (simplified here as deltas at the selected instant).</li>
          </ul>
        </div>
      </div>
    </div>
  );
}
