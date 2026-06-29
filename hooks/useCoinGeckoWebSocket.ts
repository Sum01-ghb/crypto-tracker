"use client";
import { useEffect, useRef, useState } from "react";

const WS_BASE = `${process.env.NEXT_PUBLIC_COINGECKO_WEBSOCKET_URL}?x_cg_pro_api_key=${process.env.NEXT_PUBLIC_COINGECKO_API_KEY}`;

export const useCoinGeckoWebSocket = ({
  coinId,
  poolId,
  liveInterval,
}: UseCoinGeckoWebSocketProps): UseCoinGeckoWebSocketReturn => {
  const wsRef = useRef<WebSocket | null>(null);
  const subscribed = useRef<Set<string>>(new Set());

  const [price, setPrice] = useState<ExtendedPriceData | null>(null);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [ohlcv, setOhlcv] = useState<OHLCData | null>(null);

  const [isWsReady, setIsWsReady] = useState(false);

  const fetchRecentTrades = async () => {
    if (!poolId) return;

    const [network, address] = poolId.split("_");

    if (!network || !address) return;

    try {
      const response = await fetch(
        `https://pro-api.coingecko.com/api/v3/onchain/networks/${network}/pools/${address}/trades`,
        {
          headers: {
            "x-cg-pro-api-key": process.env
              .NEXT_PUBLIC_COINGECKO_API_KEY as string,
          },
        },
      );

      if (!response.ok) {
        throw new Error(await response.text());
      }

      const json = await response.json();

      const latestTrades: Trade[] = (json.data ?? [])
        .slice(0, 7)
        .map((trade: any) => ({
          price: Number(trade.attributes.price_to_in_usd),
          value: Number(trade.attributes.volume_in_usd),
          timestamp: new Date(trade.attributes.block_timestamp).getTime(),
          type: trade.attributes.kind,
          amount: Number(trade.attributes.to_token_amount),
        }));

      setTrades(latestTrades);
    } catch (err) {
      console.error("Failed to fetch recent trades:", err);
    }
  };

  useEffect(() => {
    const ws = new WebSocket(WS_BASE);
    wsRef.current = ws;

    const send = (payload: Record<string, unknown>) =>
      ws.send(JSON.stringify(payload));

    const handleMessage = (event: MessageEvent) => {
      const msg: WebSocketMessage = JSON.parse(event.data);

      if (msg.type === "ping") {
        send({ type: "pong" });
        return;
      }

      if (msg.type === "confirm_subscription") {
        const { channel } = JSON.parse(msg.identifier ?? "{}");
        subscribed.current.add(channel);
      }

      if (msg.c === "C1") {
        setPrice({
          usd: msg.p ?? 0,
          coin: msg.i,
          price: msg.p,
          change24h: msg.pp,
          marketCap: msg.m,
          volume24h: msg.v,
          timestamp: msg.t,
        });
      }

      if (msg.ch === "G3") {
        const candle: OHLCData = [
          msg.t ?? 0,
          Number(msg.o ?? 0),
          Number(msg.h ?? 0),
          Number(msg.l ?? 0),
          Number(msg.c ?? 0),
        ];

        setOhlcv(candle);
      }
    };

    ws.onopen = () => setIsWsReady(true);
    ws.onmessage = handleMessage;
    ws.onclose = () => setIsWsReady(false);
    ws.onerror = (error) => {
      console.error("Websocket error: ", error);
      setIsWsReady(false);
    };

    return () => ws.close();
  }, []);

  useEffect(() => {
    if (!isWsReady) return;

    const ws = wsRef.current;
    if (!ws) return;

    const send = (payload: Record<string, unknown>) =>
      ws.send(JSON.stringify(payload));

    const unsubscribeAll = () => {
      subscribed.current.forEach((channel) => {
        send({
          command: "unsubscribe",
          identifier: JSON.stringify({ channel }),
        });
      });

      subscribed.current.clear();
    };

    const subscribe = (channel: string, data?: Record<string, unknown>) => {
      if (subscribed.current.has(channel)) return;

      send({
        command: "subscribe",
        identifier: JSON.stringify({ channel }),
      });

      if (data) {
        send({
          command: "message",
          identifier: JSON.stringify({ channel }),
          data: JSON.stringify(data),
        });
      }
    };

    queueMicrotask(() => {
      setPrice(null);
      setOhlcv(null);

      unsubscribeAll();

      subscribe("CGSimplePrice", {
        coin_id: [coinId],
        action: "set_tokens",
      });
    });

    const poolAddress = poolId.replace("_", ":") ?? "";

    if (poolAddress) {
      subscribe("OnchainOHLCV", {
        "network_id:pool_addresses": [poolAddress],
        interval: liveInterval,
        action: "set_pools",
      });
    }
  }, [coinId, poolId, liveInterval, isWsReady]);

  useEffect(() => {
    if (!poolId) return;

    fetchRecentTrades();

    const interval = setInterval(() => {
      fetchRecentTrades();
    }, 15000);

    return () => clearInterval(interval);
  }, [poolId]);

  return {
    price,
    trades,
    ohlcv,
    isConnected: isWsReady,
  };
};
