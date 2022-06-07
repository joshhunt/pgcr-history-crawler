import sqlite3 from "sqlite3";
import PQueue from "p-queue";
import { DestinyPostGameCarnageReportData } from "bungie-api-ts/destiny2";
import { add, formatRelative } from "date-fns";
import { setTimeout as wait } from "timers/promises";
import Keyv from "keyv";
import https from "https";
import dotenv from "dotenv";
import axios from "axios";
import promClient, { register, collectDefaultMetrics } from "prom-client";
import { ServerResponse } from "bungie-api-ts/common";
import express from "express";

import _ from "parse-duration";

dotenv.config();

collectDefaultMetrics();

const pgcrSuccessMetric = new promClient.Counter({
  name: "temp_pgcr_success",
  help: "temp_pgcr_success",
});

const pgcrErrorMetric = new promClient.Counter({
  name: "temp_pgcr_error",
  help: "temp_pgcr_error",
  labelNames: ["bungie_error"] as const,
});

const pgcrGiveUpMetric = new promClient.Counter({
  name: "temp_pgcr_give_up",
  help: "temp_pgcr_give_up",
});

const concurrencyMetric = new promClient.Gauge({
  name: "temp_pgcr_concurrency",
  help: "temp_pgcr_concurrency",
});

const pgcrRefetchTotal = new promClient.Gauge({
  name: "temp_pgcr_refetch_total",
  help: "temp_pgcr_refetch_total",
});

const pgcrRefetchQueueLength = new promClient.Gauge({
  name: "temp_pgcr_refetch_queue_length",
  help: "temp_pgcr_refetch_queue_length",
});

const API_KEY =
  process.env.BUNGIE_API_KEY ?? "529cac5f9e3a482b86b931f1f75f2331";

const COLLECT_PERIOD_START = new Date("2022-05-31T17:00:00Z");
const COLLECT_PERIOD_END = new Date(COLLECT_PERIOD_START);
COLLECT_PERIOD_END.setDate(COLLECT_PERIOD_END.getDate() + 7);

const IPV6_BASE = process.env.IPV6_BASE;
const INITIAL_CONCURRENCY = parseInt(process.env.INITIAL_CONCURRENCY || "50");
const MAX_CONCURRENCY = parseInt(process.env.MAX_CONCURRENCY || "50");

const THIRTY_SECONDS = _("30 sec");
const ONE_SECOND = _("1 sec");
const FIVE_SECOND = _("5 sec");

let httpsAgents: https.Agent[];

if (IPV6_BASE) {
  httpsAgents = "123456789abcdef"
    .split("")
    .map((v) => new https.Agent({ localAddress: IPV6_BASE + v, family: 6 }));
} else {
  httpsAgents = [new https.Agent()];
}

let lastConcurrencyChange = new Date();
let lastThrottle = 0;

const keyv = new Keyv("sqlite://keyval.sqlite");
const db = new sqlite3.Database("./keyval.sqlite");

const re = /keyv:pgcr-(\d+)/;

const IB_HASHES = [
  2965669871, // IB Freelance Rift
  2393304349, // IB Rift
];

const queue = new PQueue({ concurrency: INITIAL_CONCURRENCY });
concurrencyMetric.set(queue.concurrency);

function makePgcrWorker(pgcrId: number) {
  return async () => {
    if (HAS_REQUESTED_ABORT) {
      console.log(
        `Not fetching PGCR ${pgcrId} because a shutdown was requested`
      );
      return;
    }

    let pgcr: DestinyPostGameCarnageReportData;

    if (lastThrottle > 0 && Date.now() - lastThrottle < FIVE_SECOND) {
      console.log("We throttled recently, so going to wait a second");
      await wait(ONE_SECOND);
    }

    try {
      pgcr = await fetchPgcrWithRetries(pgcrId);
    } catch (err) {
      pgcrGiveUpMetric.inc();
      console.log(`*** Error fetching PGCR ${pgcrId}: ${err}. Skipping ***`);
      return;
    }

    pgcrSuccessMetric.inc();

    const pgcrPeriod = new Date(pgcr.period);
    const isIronBanner = IB_HASHES.includes(
      pgcr.activityDetails.directorActivityHash
    );

    if (isIronBanner) {
      console.log(
        `PGCR ${pgcr.activityDetails.instanceId} - ${formatRelative(
          pgcrPeriod,
          new Date()
        )}`
      );

      const teams: Record<string, { standing: string; score: number }> = {};

      for (const team of pgcr.teams) {
        teams[team.teamName] = {
          standing: team.standing.basic.displayValue,
          score: team.score.basic.value,
        };
      }

      const duration =
        pgcr.entries[0]?.values?.activityDurationSeconds.basic.value ?? -1;

      const key = `pgcr-${pgcrId.toString()}`;
      keyv.set(key, { teams, duration, period: pgcrPeriod });
    }

    const timeSinceLastThrottle = Date.now() - lastThrottle;
    const timeSinceLastConcurrencyChange =
      Date.now() - lastConcurrencyChange.getTime();

    if (
      queue.concurrency < MAX_CONCURRENCY &&
      timeSinceLastThrottle > THIRTY_SECONDS &&
      timeSinceLastConcurrencyChange > THIRTY_SECONDS
    ) {
      lastConcurrencyChange = new Date();
      queue.concurrency = queue.concurrency + 1;
      concurrencyMetric.set(queue.concurrency);
      console.log("Increasing concurrency to", queue.concurrency);
    }
  };
}

export async function bungieHttp<T>(url: string): Promise<T> {
  const httpsAgent =
    httpsAgents[Math.floor(Math.random() * httpsAgents.length)];

  const resp = await axios.get<ServerResponse<T>>(url, {
    httpsAgent,
    headers: {
      "x-api-key": API_KEY,
    },
  });

  return resp.data.Response;
}

async function fetchPgcr(
  pgcrId: number
): Promise<DestinyPostGameCarnageReportData> {
  try {
    return await bungieHttp<DestinyPostGameCarnageReportData>(
      `https://stats.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/${pgcrId}/`
    );
  } catch (err: any) {
    if (err?.response?.data?.ErrorStatus) {
      const exception = new Error(
        `${err?.response?.data?.ErrorStatus}: ${err?.response?.data?.Message}`
      );
      (exception as any).bungieErrorStatus = err?.response?.data?.ErrorStatus;
      throw exception;
    }
    throw err;
  }
}

async function fetchPgcrWithRetries(pgcrId: number) {
  let retries = 0;
  let lastError: any;

  while (retries < 10) {
    retries += 1;

    try {
      return await fetchPgcr(pgcrId);
    } catch (err: any) {
      console.log(`PGCR ${pgcrId} error: ${err} Retry ${retries}`);

      pgcrErrorMetric.inc({ bungie_error: err.bungieErrorStatus });

      const timeSinceLastConcurrencyChange =
        Date.now() - lastConcurrencyChange.getTime();

      if (timeSinceLastConcurrencyChange > ONE_SECOND) {
        lastThrottle = Date.now();
        lastConcurrencyChange = new Date();
        queue.concurrency = Math.max(1, queue.concurrency - 5);
        concurrencyMetric.set(queue.concurrency);
        console.log("Reducing concurrency to", queue.concurrency);
      }

      await wait(1 * retries * 1000);
      lastError = err;
    }
  }

  throw lastError;
}

// 162033

let HAS_REQUESTED_ABORT = false;

process.on("SIGINT", async function () {
  HAS_REQUESTED_ABORT = true;
  console.log("\nGracefully shutting down from SIGINT (Ctrl+C)");

  console.log("Clearing the queue");
  queue.clear();

  console.log("Pausing execution");
  queue.pause();

  console.log("Waiting for idle");
  await queue.onIdle();

  console.log("Exiting");
  process.exit();
});

let added = 0;

db.each(
  "select key from keyv where value NOT LIKE '%period%'",
  (err, row) => {
    if (HAS_REQUESTED_ABORT) {
      return;
    }

    if (err) {
      console.log(err);
    }

    if (!row.key.startsWith("keyv:pgcr-")) return;
    const match = (row.key as string).match(re);

    if (match?.[1]) {
      const pgcrId = parseInt(match?.[1]);
      queue.add(makePgcrWorker(pgcrId));
      added += 1;
    }
  },
  () => {
    pgcrRefetchTotal.set(added);
    console.log(`Added ${added} PGCRs to recheck`);
  }
);

const server = express();

server.get("/metrics", async (req, res) => {
  pgcrRefetchQueueLength.set(queue.size);

  try {
    console.log("Metrics were scraped");
    res.set("Content-Type", register.contentType);
    res.end(await register.metrics());
  } catch (ex) {
    res.status(500).end(ex);
  }
});

const port = process.env.PORT || 3000;
console.log(
  `Server listening to ${port}, metrics exposed on /metrics endpoint`
);
server.listen(port);
