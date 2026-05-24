// Top-level socket wiring: creates the io server, owns join/signal/disconnect
// + the waiting room, and delegates chat / SFU events to the handler modules.
// First socket in a room is its host; everyone else lands in the waiting room.
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import logger from "../utils/logger.js";
import { isSfuAvailable } from "../sfu/worker.js";
import { SfuRoom } from "../sfu/room.js";
import { getRedis } from "../utils/redis.js";
import {
    rooms, messages, roomLastActivity,
    socketRoom, sfuRooms, clearRateLimitState,
    roomHosts, waitingRoom,
} from "./socketHandlers/state.js";
import { registerChatHandlers } from "./socketHandlers/chat.js";
import { registerSfuHandlers } from "./socketHandlers/sfu.js";
import * as store from "../store/roomStore.js";


// Re-exported so existing tests keep working without path churn.
export { isRateLimited } from "./socketHandlers/state.js";

// Fire-and-forget wrapper for Redis writes. We don't await these because the
// in-memory state is the source of truth — Redis is just for cross-instance
// sharing — but we still want failures in the log.
const sync = (promise) => promise.catch(err =>
    logger.warn("Redis store write failed", { error: err.message }));

function normalizePath(rawPath) {
    let path = String(rawPath);
    try { path = new URL(path).pathname; } catch { /* already a pathname */ }
    return path.toLowerCase().replace(/\/+$/, "") || "/";
}

// Used both for the auto-host join and when an admitted user enters the room.
async function addUserToRoom(socket, io, path, username, avatar) {
    const sfuEnabled = isSfuAvailable();

    socket.join(path);
    socketRoom.set(socket.id, path);

    if (!rooms.has(path)) rooms.set(path, new Map());
    rooms.get(path).set(socket.id, { username, avatar });
    roomLastActivity.set(path, Date.now());

    sync(store.setSocketRoom(socket.id, path));
    sync(store.addParticipant(path, socket.id, { username, avatar }));
    sync(store.setActivity(path));

    const participants = [...rooms.get(path)].map(([sid, info]) => ({ socketId: sid, ...info }));

    if (sfuEnabled) {
        try {
            if (!sfuRooms.has(path)) {
                const sfuRoom = new SfuRoom(path);
                await sfuRoom.init();
                sfuRooms.set(path, sfuRoom);
            }
            sfuRooms.get(path).addPeer(socket.id);
        } catch (err) {
            logger.error("SFU room init failed", { error: err.message });
        }
    }

    io.to(path).emit("user-joined", socket.id, participants);

    // Catch the new joiner up on chat history.
    if (messages.has(path)) {
        for (const msg of messages.get(path)) {
            socket.emit("chat-message", msg.data, msg.sender, msg.socketId, msg.timestamp);
        }
    }

    logger.info("User joined room", {
        socketId: socket.id, username,
        room: path.slice(-20), participants: participants.length, sfu: sfuEnabled,
    });
}

function sendWaitingListToHost(io, path) {
    const hostId = roomHosts.get(path);
    if (!hostId) return;
    const waiting = waitingRoom.get(path);
    const list = waiting ? [...waiting].map(([sid, info]) => ({ socketId: sid, ...info })) : [];
    io.to(hostId).emit("waiting-room-update", list);
}

export const connectToSocket = async (server) => {
    const io = new Server(server, {
        cors: {
            origin: true,
            methods: ["GET", "POST"],
            credentials: true,
        },
        pingInterval: 25_000,
        pingTimeout: 20_000,
    });

    // Wire up the Redis adapter before any sockets connect, otherwise the
    // first connection misses cross-instance broadcasts.
    const redis = getRedis();
    if (redis) {
        try {
            const pubClient = redis.duplicate();
            const subClient = redis.duplicate();
            await Promise.all([pubClient.connect(), subClient.connect()]);
            io.adapter(createAdapter(pubClient, subClient));
            logger.info("Socket.IO Redis adapter attached");
        } catch (err) {
            logger.warn("Socket.IO Redis adapter failed, using in-memory", { error: err.message });
        }
    }

    io.on("connection", (socket) => {
        // Mirror HTTP's x-request-id onto the socket for log correlation.
        socket.requestId = socket.handshake.headers["x-request-id"] || socket.id;
        logger.info("Socket connected", { socketId: socket.id, requestId: socket.requestId, sfu: isSfuAvailable() });

        socket.on("join-call", async (rawPath, username = "Guest", avatar = "😊") => {
            username = String(username).slice(0, 40) || "Guest";
            avatar   = String(avatar).slice(0, 4)   || "😊";
            const path = normalizePath(rawPath);

            leaveCurrentRoom(socket, io);

            // First socket in an empty room is always the host and skips the lobby.
            if (!roomHosts.has(path) || !rooms.has(path) || rooms.get(path).size === 0) {
                roomHosts.set(path, socket.id);
                sync(store.setHost(path, socket.id));
                await addUserToRoom(socket, io, path, username, avatar);
                socket.emit("host-status", true);
                logger.info("User is host", { socketId: socket.id, room: path.slice(-20) });
                return;
            }

            // Otherwise drop into the waiting room until the host admits them.
            if (!waitingRoom.has(path)) waitingRoom.set(path, new Map());
            waitingRoom.get(path).set(socket.id, { username, avatar });
            // socketRoom is set here too so disconnect cleanup still finds the path.
            socketRoom.set(socket.id, path);
            sync(store.addToWaitingRoom(path, socket.id, { username, avatar }));
            sync(store.setSocketRoom(socket.id, path));

            socket.emit("waiting-room-status", { status: "waiting" });
            sendWaitingListToHost(io, path);
            logger.info("User in waiting room", { socketId: socket.id, username, room: path.slice(-20) });
        });

        socket.on("admit-user", async (targetSocketId) => {
            const path = socketRoom.get(socket.id);
            if (!path || roomHosts.get(path) !== socket.id) return;

            const waiting = waitingRoom.get(path);
            if (!waiting || !waiting.has(targetSocketId)) return;

            const { username, avatar } = waiting.get(targetSocketId);
            waiting.delete(targetSocketId);
            if (waiting.size === 0) waitingRoom.delete(path);

            // Wipe the temporary mapping so addUserToRoom can write a fresh one.
            socketRoom.delete(targetSocketId);
            sync(store.removeFromWaitingRoom(path, targetSocketId));
            sync(store.deleteSocketRoom(targetSocketId));

            const targetSocket = io.sockets.sockets.get(targetSocketId);
            if (!targetSocket) return;

            targetSocket.emit("waiting-room-status", { status: "admitted" });
            await addUserToRoom(targetSocket, io, path, username, avatar);
            sendWaitingListToHost(io, path);
        });

        socket.on("reject-user", (targetSocketId) => {
            const path = socketRoom.get(socket.id);
            if (!path || roomHosts.get(path) !== socket.id) return;

            const waiting = waitingRoom.get(path);
            if (!waiting || !waiting.has(targetSocketId)) return;

            waiting.delete(targetSocketId);
            if (waiting.size === 0) waitingRoom.delete(path);

            socketRoom.delete(targetSocketId);
            sync(store.removeFromWaitingRoom(path, targetSocketId));
            sync(store.deleteSocketRoom(targetSocketId));

            const targetSocket = io.sockets.sockets.get(targetSocketId);
            if (targetSocket) {
                targetSocket.emit("waiting-room-status", { status: "rejected" });
            }
            sendWaitingListToHost(io, path);
        });

        socket.on("admit-all", async () => {
            const path = socketRoom.get(socket.id);
            if (!path || roomHosts.get(path) !== socket.id) return;

            const waiting = waitingRoom.get(path);
            if (!waiting) return;

            for (const [sid, { username, avatar }] of [...waiting]) {
                waiting.delete(sid);
                socketRoom.delete(sid);
                sync(store.deleteSocketRoom(sid));
                const targetSocket = io.sockets.sockets.get(sid);
                if (targetSocket) {
                    targetSocket.emit("waiting-room-status", { status: "admitted" });
                    await addUserToRoom(targetSocket, io, path, username, avatar);
                }
            }
            waitingRoom.delete(path);
            sync(store.clearWaitingRoom(path));
            sendWaitingListToHost(io, path);
        });

        // P2P signal relay.
        socket.on("signal", (toId, message) => {
            io.to(toId).emit("signal", socket.id, message);
        });

        registerChatHandlers(socket, io);
        registerSfuHandlers(socket, io);

        socket.on("disconnect", () => {
            logger.info("Socket disconnected", { socketId: socket.id });
            leaveCurrentRoom(socket, io);
            clearRateLimitState(socket.id);
        });
    });

    return io;
};

function leaveCurrentRoom(socket, io) {
    const path = socketRoom.get(socket.id);
    if (!path) return;

    socketRoom.delete(socket.id);
    sync(store.deleteSocketRoom(socket.id));

    // If they were only in the waiting room, no peer cleanup is needed.
    const waiting = waitingRoom.get(path);
    if (waiting && waiting.has(socket.id)) {
        waiting.delete(socket.id);
        if (waiting.size === 0) waitingRoom.delete(path);
        sync(store.removeFromWaitingRoom(path, socket.id));
        sendWaitingListToHost(io, path);
        socket.leave(path);
        return;
    }

    const sfuRoom = sfuRooms.get(path);
    if (sfuRoom) {
        const closedProducerIds = sfuRoom.removePeer(socket.id);
        for (const producerId of closedProducerIds) {
            io.to(path).emit("producer-closed", { producerId });
        }
        if (sfuRoom.isEmpty()) {
            sfuRoom.close();
            sfuRooms.delete(path);
        }
    }

    const room = rooms.get(path);
    if (room) {
        room.delete(socket.id);
        sync(store.removeParticipant(path, socket.id));
        io.to(path).emit("user-left", socket.id);

        // If the host left, hand the role to whoever is next in the room.
        if (roomHosts.get(path) === socket.id) {
            if (room.size > 0) {
                const newHostId = room.keys().next().value;
                roomHosts.set(path, newHostId);
                sync(store.setHost(path, newHostId));
                io.to(newHostId).emit("host-status", true);
                sendWaitingListToHost(io, path);
                logger.info("Host promoted", { newHost: newHostId, room: path.slice(-20) });
            } else {
                roomHosts.delete(path);
                sync(store.deleteHost(path));
                // Nobody left to host; reject anyone still in the lobby.
                const pendingWaiting = waitingRoom.get(path);
                if (pendingWaiting && pendingWaiting.size > 0) {
                    for (const [sid] of pendingWaiting) {
                        const ws = io.sockets.sockets.get(sid);
                        if (ws) ws.emit("waiting-room-status", { status: "rejected" });
                        socketRoom.delete(sid);
                        sync(store.deleteSocketRoom(sid));
                    }
                    waitingRoom.delete(path);
                    sync(store.clearWaitingRoom(path));
                }
            }
        }

        if (room.size === 0) {
            rooms.delete(path);
            messages.delete(path);
            roomLastActivity.delete(path);
            roomHosts.delete(path);
            waitingRoom.delete(path);
            sync(store.deleteRoom(path));
        }
    }

    socket.leave(path);
}
