import https from "https";
import dotenv from "dotenv";
import axios from "axios";
import { ServerResponse } from "bungie-api-ts/common";

dotenv.config();

const IPV6_BASE = process.env.IPV6_BASE;
const API_KEY =
  process.env.BUNGIE_API_KEY ?? "529cac5f9e3a482b86b931f1f75f2331";

let httpsAgents: https.Agent[];

if (IPV6_BASE) {
  httpsAgents = "123456789abcdef"
    .split("")
    .map((v) => new https.Agent({ localAddress: v + IPV6_BASE, family: 6 }));
} else {
  httpsAgents = [new https.Agent()];
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
