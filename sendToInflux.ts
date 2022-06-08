import sqlite3 from "sqlite3";
import PQueue from "p-queue";
import {
  DestinyPostGameCarnageReportData,
  DestinyPostGameCarnageReportEntry,
} from "bungie-api-ts/destiny2";
import { add, formatRelative } from "date-fns";
import { setTimeout as wait } from "timers/promises";
import Keyv from "keyv";
import https from "https";
import dotenv from "dotenv";
import axios from "axios";
import promClient, { register, collectDefaultMetrics } from "prom-client";
import { ServerResponse } from "bungie-api-ts/common";
import express from "express";
import { InfluxDB, Point } from "@influxdata/influxdb-client";

import _ from "parse-duration";

dotenv.config();

collectDefaultMetrics();

const client = new InfluxDB({
  url: "https://us-east-1-1.aws.cloud2.influxdata.com",
  token: process.env.INFLUXDB_TOKEN,
});

const influxSuccessMetric = new promClient.Counter({
  name: "temp_pgcr_influx_success",
  help: "temp_pgcr_influx_success",
});

const influxErrorMetric = new promClient.Counter({
  name: "temp_pgcr_influx_error",
  help: "temp_pgcr_influx_error",
});

const influxGiveUpMetric = new promClient.Counter({
  name: "temp_pgcr_influx_give_up",
  help: "temp_pgcr_influx_give_up",
});

const concurrencyMetric = new promClient.Gauge({
  name: "temp_pgcr_concurrency",
  help: "temp_pgcr_concurrency",
});

const pgcrInfluxTotal = new promClient.Gauge({
  name: "temp_pgcr_influx_total",
  help: "temp_pgcr_influx_total",
});

const pgcrInfluxQueueLength = new promClient.Gauge({
  name: "temp_pgcr_influx_queue_length",
  help: "temp_pgcr_influx_queue_length",
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

interface TeamData {
  standing: string;
  score: number;
  largestFireteamSize: number;
}

interface SavedPGCRData {
  teams: Record<string, TeamData>;
  duration: number;
  period: Date;
}

let org = `josh@trtr.co`;
let bucket = process.env.INFLUX_BUCKET || `test-rifts`;

let writeClient = client.getWriteApi(org, bucket, "s");

setInterval(async () => {
  console.log("flushing to influxdb");
  await writeClient.flush();
  console.log("flushed.");
}, 1000);

function makePgcrWorker(pgcrId: number) {
  return async () => {
    if (HAS_REQUESTED_ABORT) {
      console.log(
        `Not fetching PGCR ${pgcrId} because a shutdown was requested`
      );
      return;
    }

    const key = `pgcr-${pgcrId.toString()}`;
    const savedPgcrData: SavedPGCRData = await keyv.get(key);

    let winningTeam: TeamData;
    let losingTeam: TeamData;
    let isTied = false;

    const [teamA, teamB] = Object.values(savedPgcrData.teams);
    const pgcrDate = new Date(savedPgcrData.period);

    if (!teamA || !teamB) {
      console.log(`PGCR ${pgcrId} does not have both teams`, savedPgcrData);
      return Promise.resolve();
    }

    if (teamA.score === teamB.score) {
      isTied = true;
      winningTeam = teamA;
      losingTeam = teamB;
    } else if (teamA.score > teamB.score) {
      winningTeam = teamA;
      losingTeam = teamB;
    } else {
      winningTeam = teamB;
      losingTeam = teamA;
    }

    let point = new Point("pgcr")
      .timestamp(pgcrDate)
      .tag("isTied", isTied ? "true" : "false")
      .tag("pgcrId", pgcrId.toString())
      .intField("winningTeamScore", winningTeam.score)
      .intField("winningTeamLargestFireteam", winningTeam.largestFireteamSize)
      .stringField("winningTeamStanding", winningTeam.standing)
      .intField("losingTeamScore", losingTeam.score)
      .intField("losingTeamLargestFireteam", losingTeam.largestFireteamSize)
      .stringField("losingTeamStanding", losingTeam.standing);

    writeClient.writePoint(point);
    console.log(`PGCR ${pgcrId} point written`);

    return await wait(100);
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

      influxErrorMetric.inc({ bungie_error: err.bungieErrorStatus });

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
  "select key from keyv where value LIKE '%period%' AND value LIKE '%largestFireteamSize%'",
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
    pgcrInfluxTotal.set(added);
    console.log(`Added ${added} PGCRs to recheck`);
  }
);

const server = express();

server.get("/metrics", async (req, res) => {
  pgcrInfluxQueueLength.set(queue.size);

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

for (let i = 0; i < 5; i++) {
  let point = new Point("measurement1")
    .tag("tagname1", "tagvalue1")
    .intField("field1", i);

  void setTimeout(() => {
    console.log("writing test point");
    writeClient.writePoint(point);
  }, i * 1000); // separate points by 1 second

  void setTimeout(() => {
    console.log("flushing test point");
    writeClient.flush();
  }, 5000);
}
