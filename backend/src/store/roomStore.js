// Redis-backed mirror of the in-memory room state so multiple server instances
// can share rooms. Every export is a no-op when Redis isn't connected.
import { getRedis } from "../utils/redis.js";
import logger from "../utils/logger.js";

const KEY_TTL = 86400; // matches the in-memory 24h TTL
const MAX_MESSAGES = 200;
const MESSAGE_RATE_LIMIT = 10;
const MESSAGE_RATE_WINDOW = 10; // seconds

const rk = (...parts) => parts.join(":");

// Wraps a function so it short-circuits to `null` when Redis isn't available.
// The wrapped fn receives the redis client as its first argument.
const withRedis = (fn) => async (...args) => {
    const redis = getRedis();
    if (!redis) return null;
    return fn(redis, ...args);
};

// participants

export const addParticipant = withRedis(async (redis, path, socketId, { username, avatar }) => {
    const key = rk("room", path, "participants");
    await redis.hSet(key, socketId, JSON.stringify({ username, avatar }));
    await redis.expire(key, KEY_TTL);
});

export const removeParticipant = withRedis((redis, path, socketId) =>
    redis.hDel(rk("room", path, "participants"), socketId));

export const getParticipants = withRedis(async (redis, path) => {
    const raw = await redis.hGetAll(rk("room", path, "participants"));
    if (!raw || Object.keys(raw).length === 0) return null;
    return new Map(Object.entries(raw).map(([sid, json]) => [sid, JSON.parse(json)]));
});

export const getParticipantCount = withRedis((redis, path) =>
    redis.hLen(rk("room", path, "participants")));

// host

export const setHost = withRedis((redis, path, socketId) =>
    redis.set(rk("room", path, "host"), socketId, { EX: KEY_TTL }));

export const getHost = withRedis((redis, path) =>
    redis.get(rk("room", path, "host")));

export const deleteHost = withRedis((redis, path) =>
    redis.del(rk("room", path, "host")));

// waiting room

export const addToWaitingRoom = withRedis(async (redis, path, socketId, { username, avatar }) => {
    const key = rk("room", path, "waiting");
    await redis.hSet(key, socketId, JSON.stringify({ username, avatar }));
    await redis.expire(key, KEY_TTL);
});

export const removeFromWaitingRoom = withRedis((redis, path, socketId) =>
    redis.hDel(rk("room", path, "waiting"), socketId));

export const getWaitingRoom = withRedis(async (redis, path) => {
    const raw = await redis.hGetAll(rk("room", path, "waiting"));
    if (!raw || Object.keys(raw).length === 0) return null;
    return new Map(Object.entries(raw).map(([sid, json]) => [sid, JSON.parse(json)]));
});

export const clearWaitingRoom = withRedis((redis, path) =>
    redis.del(rk("room", path, "waiting")));

// socket -> room mapping

export const setSocketRoom = withRedis((redis, socketId, path) =>
    redis.set(rk("socket", socketId, "room"), path, { EX: KEY_TTL }));

export const getSocketRoom = withRedis((redis, socketId) =>
    redis.get(rk("socket", socketId, "room")));

export const deleteSocketRoom = withRedis((redis, socketId) =>
    redis.del(rk("socket", socketId, "room")));

// chat messages

export const pushMessage = withRedis(async (redis, path, msg) => {
    const key = rk("room", path, "messages");
    await redis.rPush(key, JSON.stringify(msg));
    await redis.lTrim(key, -MAX_MESSAGES, -1);
    await redis.expire(key, KEY_TTL);
});

export const getMessages = withRedis(async (redis, path) => {
    const raw = await redis.lRange(rk("room", path, "messages"), 0, -1);
    return raw.map(json => JSON.parse(json));
});

// activity tracking

export const setActivity = withRedis((redis, path) =>
    redis.set(rk("room", path, "activity"), Date.now().toString(), { EX: KEY_TTL }));

// rate limiting

export const isRateLimitedRedis = withRedis(async (redis, socketId) => {
    const key = rk("ratelimit", socketId);
    const now = Date.now();
    await redis.zRemRangeByScore(key, "-inf", String(now - MESSAGE_RATE_WINDOW * 1000));
    const count = await redis.zCard(key);
    if (count >= MESSAGE_RATE_LIMIT) return true;
    await redis.zAdd(key, { score: now, value: String(now) });
    await redis.expire(key, MESSAGE_RATE_WINDOW);
    return false;
});

// room cleanup

export const deleteRoom = withRedis(async (redis, path) => {
    await Promise.all([
        redis.del(rk("room", path, "participants")),
        redis.del(rk("room", path, "host")),
        redis.del(rk("room", path, "waiting")),
        redis.del(rk("room", path, "messages")),
        redis.del(rk("room", path, "activity")),
    ]);
    logger.info("Redis room keys cleaned", { room: path.slice(-20) });
});

export const clearRateLimitRedis = withRedis((redis, socketId) =>
    redis.del(rk("ratelimit", socketId)));
