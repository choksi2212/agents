import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { StreamBenchObject } from "../capabilities/streams-bench";

/**
 * The chat recovery progress marker is derived from the stream log: durably
 * flushed segments, counted from live logs while a stream's rows exist and
 * from a retired total once they are deleted. It must move only when new
 * content lands, never when rows are merely read or removed.
 */
describe("ResumableStream.progressMarker", () => {
  it("counts flushed segments, survives cutover, reclaim and clear, and never moves on replay", async () => {
    const stub = env.StreamBenchObject.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance: StreamBenchObject) => {
      const { marker, rowsWritten } = await instance.probeProgressMarker();

      // Opening a stream is not progress; only flushed segments are.
      expect(marker.fresh).toBe(0);
      expect(marker.opened).toBe(0);
      // 25 chunks: two packed segments landed, five are still buffered.
      expect(marker.buffered).toBe(2);
      expect(marker.flushed).toBe(3);
      // A replay reads the log; it appends nothing.
      expect(marker.replayed).toBe(3);
      // The cutover deleted the rows and retired their segments in the same
      // transaction: the marker is exactly where it was.
      expect(marker.cutOver).toBe(3);

      // The second turn's single segment settled with its row still present.
      expect(marker.completed).toBe(4);
      // start() reclaimed that row; its segment moved to the retired total.
      expect(marker.reclaimed).toBe(4);
      expect(marker.thirdFlushed).toBe(5);

      // One explicit credit (a forwarded child chunk) is one unit.
      expect(marker.credited).toBe(6);
      // Clearing history retires the live stream's segment; nothing is lost.
      expect(marker.cleared).toBe(6);

      // Seeding folds the counter in by max against the row's own durable
      // total: a lower counter changes nothing, a counter past the derived
      // total raises the marker to itself.
      expect(marker.seededLow).toBe(6);
      expect(marker.seededHigh).toBe(46);

      // No per-chunk write exists: 36 chunks were stored along the way, and
      // the whole sequence — stream opens, block writes, settles, deletions
      // with their retire rows, one credit, and the seeds — is a fixed count
      // well under one row per chunk.
      expect(rowsWritten).toBeLessThan(40);
    });
  });

  it("keeps the marker when a chat row is deleted through the public capability", async () => {
    const stub = env.StreamBenchObject.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance: StreamBenchObject) => {
      const { before, after } = await instance.probePublicDelete();
      expect(before).toBe(1);
      expect(after).toBe(1);
    });
  });

  it("reports the durable marker to the host per retire and per credit, never per chunk", async () => {
    const stub = env.StreamBenchObject.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance: StreamBenchObject) => {
      await instance.probeProgressMarker();
      // Cutover, reclaim, credit, clear: four reports for 36 stored chunks,
      // each carrying the durable total as it stood, never decreasing.
      expect(instance.progressReports).toEqual([3, 4, 5, 6]);
    });
  });

  it("retires a stream once when the adapter was constructed more than once", async () => {
    const stub = env.StreamBenchObject.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance: StreamBenchObject) => {
      const { marker, reports } = await instance.probeReconstruction();
      // Three constructions, one cutover of a one-segment stream: the
      // earlier hooks were replaced, so the segment is retired once and the
      // host hears about it once.
      expect(marker).toBe(1);
      expect(reports).toBe(1);
    });
  });

  it("does not re-add the retired total when a new isolate seeds the mirrored durable marker", async () => {
    const stub = env.StreamBenchObject.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance: StreamBenchObject) => {
      const marker = await instance.probeMirrorReseed();
      // The cutover retired a segment and the host mirrored the durable
      // total to the pre-derivation KV key; a new isolate seeds that value.
      expect(marker.retired).toBeGreaterThan(0);
      // Seeding the mirrored total is a no-op: the row already counts those
      // segments in `retired`, and folding them into `legacy` again would
      // grow the marker by the whole retired history on every wake, sealing
      // long conversations with work_budget_exceeded.
      expect(marker.reseeded).toBe(marker.retired);
      expect(marker.reseededAgain).toBe(marker.retired);
      // Rollback insurance is intact: a counter past the derived marker
      // still raises the marker to the counter, never lower.
      expect(marker.counterPastDerived).toBe(marker.reseeded + 100);
    });
  });

  it("seeds the legacy counter against segments retired before it, idempotently", async () => {
    const stub = env.StreamBenchObject.getByName(crypto.randomUUID());
    await runInDurableObject(stub, async (instance: StreamBenchObject) => {
      const marker = await instance.probeSeedOrdering();
      // Two segments were retired by a cutover before any seed landed.
      expect(marker.retiredBeforeSeed).toBe(2);
      // The seed folds the counter in by max against the row's own durable
      // total: the row already counts what it produced, so the marker
      // reads the counter, never lower and never the sum of both.
      expect(marker.seeded).toBe(40);
      // Another isolate seeding the same counter changes nothing.
      expect(marker.seededAgain).toBe(40);
      expect(marker.seededLower).toBe(40);
      expect(marker.credited).toBe(41);
    });
  });
});
