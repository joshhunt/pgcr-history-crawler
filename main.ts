import PQueue from "p-queue";
import {
  DestinyPostGameCarnageReportData,
  DestinyPostGameCarnageReportEntry,
} from "bungie-api-ts/destiny2";
import { formatRelative } from "date-fns";
import { setTimeout as wait } from "timers/promises";
import Keyv from "keyv";
import https from "https";
import dotenv from "dotenv";
import axios from "axios";
import promClient, { register, collectDefaultMetrics } from "prom-client";
import { ServerResponse } from "bungie-api-ts/common";
import express from "express";

import _ from "parse-duration";
import {
  getActivity,
  includeTables,
  load,
  setApiKey,
} from "@d2api/manifest-node";

dotenv.config();

collectDefaultMetrics();

const server = express();

server.get("/metrics", async (req, res) => {
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

const maxPgcrMetric = new promClient.Gauge({
  name: "temp_pgcr_max_crawled",
  help: "temp_pgcr_max_crawled",
});

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

const API_KEY =
  process.env.BUNGIE_API_KEY ?? "529cac5f9e3a482b86b931f1f75f2331";

const COLLECT_PERIOD_START = new Date("2022-05-31T17:00:00Z");
const COLLECT_PERIOD_END = new Date(COLLECT_PERIOD_START);
COLLECT_PERIOD_END.setDate(COLLECT_PERIOD_END.getDate() + 7);

const IPV6_BASE = process.env.IPV6_BASE;
const INITIAL_CONCURRENCY = parseInt(process.env.INITIAL_CONCURRENCY || "50");
const MAX_CONCURRENCY = parseInt(process.env.MAX_CONCURRENCY || "50");
const INITIAL_PGCR_ID = parseInt(process.env.INITIAL_PGCR_ID || "10856133731");
const MAX_PGCR_ID = parseInt(process.env.MAX_PGCR_ID || "99999999999");

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

const keyv = new Keyv("sqlite://keyval.sqlite");

const prevMaxPgcrRequested = (await keyv.get("maxPgcrRequested")) as number;

let currentPgcrId = Math.max(INITIAL_PGCR_ID, prevMaxPgcrRequested || 0);

const IB_HASHES = [
  2965669871, // IB Freelance Rift
  2393304349, // IB Rift
];

const queue = new PQueue({ concurrency: INITIAL_CONCURRENCY });
concurrencyMetric.set(queue.concurrency);

function queueNextPgcr() {
  if (currentPgcrId > MAX_PGCR_ID) {
    console.log("********************************************************");
    console.log(`********** Reached max PGCR ID ${MAX_PGCR_ID} **********`);
    console.log("********************************************************");
    return;
  }

  queue.add(makePgcrWorker(currentPgcrId));
  currentPgcrId += 1;
}

let lastConcurrencyChange = new Date();
let lastThrottle = 0;
let maxPgcrRequested = 0;

setApiKey(API_KEY);
includeTables(["Activity"]);
await load();

setInterval(() => {
  if (maxPgcrRequested > 0) {
    keyv.set("maxPgcrRequested", maxPgcrRequested);
  }
}, ONE_SECOND);

function makePgcrWorker(pgcrId: number) {
  return async () => {
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
      queueNextPgcr();
      return;
    }

    pgcrSuccessMetric.inc();

    maxPgcrRequested = Math.max(maxPgcrRequested, pgcrId);
    maxPgcrMetric.set(maxPgcrRequested);

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

      const teams: Record<
        string,
        { standing: string; score: number; largestFireteamSize: number }
      > = {};

      for (const team of pgcr.teams) {
        const teamPlayers = pgcr.entries.filter(
          (v) => v.values.team.basic.value === team.teamId
        );

        const fireteams: Record<string, DestinyPostGameCarnageReportEntry[]> =
          {};

        for (const player of teamPlayers) {
          const fireteamId = player.values.fireteamId.basic.value.toString();

          if (!fireteams[fireteamId]) {
            fireteams[fireteamId] = [];
          }

          fireteams[fireteamId].push(player);
        }

        const fireteamEntries = Object.keys(fireteams);
        teamPlayers.sort(
          (a, b) =>
            Number(a.values.fireteamId.basic.value.toString()) -
            Number(b.values.fireteamId.basic.value.toString())
        );

        for (const player of teamPlayers) {
          const fireteamIndex = fireteamEntries.indexOf(
            player.values.fireteamId.basic.value.toString()
          );
        }

        const largestFireteamSize = Math.max(
          ...Object.values(fireteams).map((v) => v.length)
        );

        teams[team.teamName] = {
          standing: team.standing.basic.displayValue,
          score: team.score.basic.value,
          largestFireteamSize,
        };
      }

      const duration =
        pgcr.entries[0]?.values?.activityDurationSeconds.basic.value ?? -1;

      const key = `pgcr-${pgcrId.toString()}`;
      const payload = { teams, duration, period: pgcrPeriod };
      keyv.set(key, payload);
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

    keyv;

    queueNextPgcr();
  };
}

for (let index = 0; index < queue.concurrency + 5; index++) {
  queueNextPgcr();
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
