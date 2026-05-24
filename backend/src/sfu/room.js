import { getNextWorker } from "./worker.js";
import { mediaCodecs, webRtcTransportOptions } from "./config.js";
import logger from "../utils/logger.js";

// One SfuRoom per meeting room. Wraps a mediasoup Router and tracks per-peer
// state.
//
// mediasoup model in one paragraph: every peer creates two WebRTC transports
// — a SEND transport (carries their outgoing tracks → producers) and a RECV
// transport (carries incoming tracks ← consumers). The Router sits in the
// middle and forwards packets between producers and consumers without
// touching media (no re-encoding), which is what lets the SFU scale past
// what a P2P mesh can handle.
export class SfuRoom {
    constructor(roomId) {
        this.roomId = roomId;
        this.router = null;
        this.peers = new Map();
    }

    async init() {
        const worker = getNextWorker();
        this.router = await worker.createRouter({ mediaCodecs });
        logger.info("SFU room created", { roomId: this.roomId.slice(-20) });
    }

    getRtpCapabilities() {
        return this.router.rtpCapabilities;
    }

    async createTransport(socketId) {
        const transportPromise = this.router.createWebRtcTransport(webRtcTransportOptions);
        const timeout = new Promise((_, reject) =>
            setTimeout(() => reject(new Error("Transport creation timed out")), 10_000));
        const transport = await Promise.race([transportPromise, timeout]);

        transport.on("dtlsstatechange", (state) => {
            if (state === "closed") {
                transport.close();
            }
        });

        return {
            id: transport.id,
            iceParameters: transport.iceParameters,
            iceCandidates: transport.iceCandidates,
            dtlsParameters: transport.dtlsParameters,
            sctpParameters: transport.sctpParameters,
            _transport: transport, // internal ref
        };
    }

    async connectTransport(socketId, transportId, dtlsParameters) {
        const peer = this.peers.get(socketId);
        if (!peer) return;

        const transport =
            peer.sendTransport?.id === transportId ? peer.sendTransport :
            peer.recvTransport?.id === transportId ? peer.recvTransport : null;

        if (transport) {
            await transport.connect({ dtlsParameters });
        }
    }

    async produce(socketId, kind, rtpParameters, appData) {
        const peer = this.peers.get(socketId);
        if (!peer?.sendTransport) return null;

        const producer = await peer.sendTransport.produce({ kind, rtpParameters, appData });

        producer.on("transportclose", () => {
            producer.close();
            peer.producers.delete(producer.id);
        });

        peer.producers.set(producer.id, producer);
        logger.debug("Producer created", { socketId, kind, producerId: producer.id });
        return producer;
    }

    async consume(socketId, producerId, rtpCapabilities) {
        if (!this.router.canConsume({ producerId, rtpCapabilities })) {
            logger.warn("Cannot consume", { socketId, producerId });
            return null;
        }

        const peer = this.peers.get(socketId);
        if (!peer?.recvTransport) return null;

        const consumer = await peer.recvTransport.consume({
            producerId,
            rtpCapabilities,
            paused: true, // client resumes once it's ready
        });

        consumer.on("transportclose", () => {
            consumer.close();
            peer.consumers.delete(consumer.id);
        });

        consumer.on("producerclose", () => {
            consumer.close();
            peer.consumers.delete(consumer.id);
        });

        peer.consumers.set(consumer.id, consumer);

        return {
            id: consumer.id,
            producerId,
            kind: consumer.kind,
            rtpParameters: consumer.rtpParameters,
            producerSocketId: consumer.appData?.socketId,
        };
    }

    addPeer(socketId) {
        this.peers.set(socketId, {
            sendTransport: null,
            recvTransport: null,
            producers: new Map(),
            consumers: new Map(),
        });
    }

    setSendTransport(socketId, transport) {
        const peer = this.peers.get(socketId);
        if (peer) peer.sendTransport = transport;
    }

    setRecvTransport(socketId, transport) {
        const peer = this.peers.get(socketId);
        if (peer) peer.recvTransport = transport;
    }

    removePeer(socketId) {
        const peer = this.peers.get(socketId);
        if (!peer) return [];

        const closedProducerIds = [];
        for (const [id, producer] of peer.producers) {
            closedProducerIds.push(id);
            producer.close();
        }
        for (const [, consumer] of peer.consumers) {
            consumer.close();
        }
        if (peer.sendTransport) peer.sendTransport.close();
        if (peer.recvTransport) peer.recvTransport.close();

        this.peers.delete(socketId);
        logger.debug("Peer removed from SFU room", { socketId, roomId: this.roomId.slice(-20) });

        return closedProducerIds;
    }

    getProducerIds(excludeSocketId) {
        const ids = [];
        for (const [socketId, peer] of this.peers) {
            if (socketId === excludeSocketId) continue;
            for (const [id, producer] of peer.producers) {
                ids.push({ producerId: id, socketId, kind: producer.kind });
            }
        }
        return ids;
    }

    async setConsumerLayers(socketId, consumerId, spatialLayer, temporalLayer) {
        const peer = this.peers.get(socketId);
        const consumer = peer?.consumers?.get(consumerId);
        if (consumer && consumer.type === 'simulcast') {
            await consumer.setPreferredLayers({ spatialLayer, temporalLayer });
        }
    }

    isEmpty() {
        return this.peers.size === 0;
    }

    close() {
        this.router.close();
        logger.info("SFU room closed", { roomId: this.roomId.slice(-20) });
    }
}
