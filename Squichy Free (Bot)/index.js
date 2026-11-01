console.clear();

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const pino = require('pino');
const chalk = require('chalk');

const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    jidDecode,
    makeInMemoryStore
} = require('@whiskeysockets/baileys');

const { smsg } = require('./myfunc');

process.on('uncaughtException', (err) => {
    console.error('[ERREUR FATALE]', err);
    process.exit(1);
});

process.on('unhandledRejection', (reason) => {
    console.error('[PROMESSE REJETÉE]', reason);
});

const PORT = process.env.SERVER_PORT || process.env.PORT || 3000;

const SESSIONS_DIR = path.join(__dirname, 'sessions');
const SETTINGS_DIR = path.join(__dirname, 'settings');

fs.mkdirSync(SESSIONS_DIR, { recursive: true });
fs.mkdirSync(SETTINGS_DIR, { recursive: true });
fs.mkdirSync(path.join(__dirname, 'm'), { recursive: true });

function getSettingsPath(numero) {
    return path.join(SETTINGS_DIR, `${numero}.json`);
}

function loadSessionSettings(numero) {
    try {
        const p = getSettingsPath(numero);

        if (fs.existsSync(p)) {
            return JSON.parse(fs.readFileSync(p, 'utf8'));
        }
    } catch (e) {
        console.error('[SETTINGS]', e);
    }

    return { public: true };
}

function saveSessionSettings(numero, patch) {
    try {
        const current = loadSessionSettings(numero);

        fs.writeFileSync(
            getSettingsPath(numero),
            JSON.stringify({ ...current, ...patch }, null, 2)
        );
    } catch (e) {
        console.error('[SAVE SETTINGS]', e);
    }
}

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());

const sessions = new Map();
const pendingCodes = new Map();
const MAX_SESSIONS = 100;

function cleanNumber(numero) {
    return String(numero || '').replace(/[^0-9]/g, '');
}

function getSessionDir(numero) {
    return path.join(SESSIONS_DIR, cleanNumber(numero));
}

function broadcastSessions() {
    const data = [];

    for (const [numero, s] of sessions.entries()) {
        data.push({
            numero,
            status: s.status,
            connectedAt: s.connectedAt,
            name: s.name || null
        });
    }

    io.emit('sessions:update', data);
}

async function loadExistingSessions() {
    if (!fs.existsSync(SESSIONS_DIR)) return;

    const dirs = fs.readdirSync(SESSIONS_DIR, {
        withFileTypes: true
    });

    for (const dir of dirs) {
        if (!dir.isDirectory()) continue;

        const credsFile = path.join(
            SESSIONS_DIR,
            dir.name,
            'creds.json'
        );

        if (fs.existsSync(credsFile)) {
            console.log(`[SESSION] Chargement : ${dir.name}`);

            await createSession(dir.name, null);
        }
    }
}

async function createSession(numero, socketId) {
    const clean = cleanNumber(numero);

    if (!clean || clean.length < 7) {
        return { success: false, error: 'invalid_number' };
    }

    const previous = sessions.get(clean) || {};

    if (previous.status === 'connected') {
        if (socketId) {
            io.to(socketId).emit('error', {
                message: 'This number is already connected!'
            });
        }

        return {
            success: false,
            error: 'already_connected'
        };
    }

    if (previous.status === 'connecting' && previous.sock) {
        return {
            success: false,
            error: 'connection_in_progress'
        };
    }

    const connectedCount = [...sessions.values()]
        .filter(s => s.status === 'connected').length;

    if (connectedCount >= MAX_SESSIONS && !sessions.has(clean)) {
        if (socketId) {
            io.to(socketId).emit('error', {
                message: 'SERVER_FULL'
            });
        }

        return {
            success: false,
            error: 'server_full'
        };
    }

    const previousRetries = previous._retries || 0;

    sessions.set(clean, {
        status: 'connecting',
        connectedAt: null,
        name: null,
        _retries: previousRetries
    });

    broadcastSessions();

    let storeClearInterval;

    try {
        const sessionDir = getSessionDir(clean);

        fs.mkdirSync(sessionDir, { recursive: true });

        const { state, saveCreds } =
            await useMultiFileAuthState(sessionDir);

        const Primis = makeWASocket({
            printQRInTerminal: false,
            syncFullHistory: false,
            markOnlineOnConnect: false,
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 0,
            keepAliveIntervalMs: 10000,
            generateHighQualityLinkPreview: true,
            browser: ['Ubuntu', 'Chrome', '20.0.04'],
            logger: pino({ level: 'silent' }),
            auth: state
        });

        const store = makeInMemoryStore({
            logger: pino({
                level: 'silent',
                stream: {
                    write: () => {}
                }
            })
        });

        store.bind(Primis.ev);

        storeClearInterval = setInterval(() => {
            try {
                store.messages?.clear?.();
                store.chats?.clear?.();
            } catch (e) {
                console.error('[STORE CLEANUP]', e);
            }
        }, 3600000);

        Primis.decodeJid = (jid) => {
            if (!jid) return jid;

            if (/:\d+@/gi.test(jid)) {
                const decode = jidDecode(jid) || {};

                return (
                    decode.user &&
                    decode.server &&
                    decode.user + '@' + decode.server
                ) || jid;
            }

            return jid;
        };

        Primis.sendText = (
            jid,
            text,
            quoted = '',
            options = {}
        ) => {
            return Primis.sendMessage(
                jid,
                { text, ...options },
                { quoted }
            );
        };

        Primis.public =
            loadSessionSettings(clean).public !== false;

        Primis.saveSetting = (key, value) => {
            saveSessionSettings(clean, {
                [key]: value
            });
        };

        let node;
        let attrs;

        Primis.ws.on('CB:message', (n) => {
            node = n.content;
            attrs = n.attrs;
        });

        if (!Primis.authState.creds.registered) {
            await new Promise(resolve => setTimeout(resolve, 3000));

            try {
                const code = await Primis.requestPairingCode(clean);

                const formatted =
                    code?.match(/.{1,4}/g)?.join('-') || code;

                pendingCodes.set(clean, formatted);

                io.to(`numero:${clean}`).emit('pairing:code', {
                    numero: clean,
                    code: formatted
                });

                if (socketId) {
                    io.to(socketId).emit('pairing:code', {
                        numero: clean,
                        code: formatted
                    });
                }

            } catch (e) {
                console.error('[PAIRING ERROR]', e);

                const errMsg = {
                    message: 'Failed to generate code. Check the number.'
                };

                io.to(`numero:${clean}`).emit('error', errMsg);

                if (socketId) {
                    io.to(socketId).emit('error', errMsg);
                }

                clearInterval(storeClearInterval);
                sessions.delete(clean);
                pendingCodes.delete(clean);
                broadcastSessions();

                return {
                    success: false,
                    error: 'pairing_code_failed'
                };
            }
        }

        Primis.ev.on('messages.upsert', async (chatUpdate) => {
            try {
                const mek = chatUpdate.messages?.[0];

                if (!mek || !mek.message) return;

                mek.message =
                    Object.keys(mek.message)[0] === 'ephemeralMessage'
                        ? mek.message.ephemeralMessage.message
                        : mek.message;

                if (
                    mek.key &&
                    mek.key.remoteJid === 'status@broadcast'
                ) {
                    await Primis.readMessages([mek.key]);
                }

                if (
                    !Primis.public &&
                    !mek.key.fromMe &&
                    chatUpdate.type === 'notify'
                ) {
                    return;
                }

                if (
                    mek.key?.id?.startsWith('D5PRIMIS') &&
                    mek.key.id.length === 16
                ) {
                    return;
                }

                let m = smsg(Primis, mek, store);

                m = {
                    ...m,
                    attributs: attrs,
                    node
                };

                require('./squichy.js')(
                    Primis,
                    m,
                    chatUpdate,
                    store
                );

            } catch (error) {
                console.error('[MESSAGE ERROR]', error);
            }
        });

        Primis.ev.on('connection.update', async (update) => {
            const {
                connection,
                lastDisconnect
            } = update;

            if (connection === 'open') {
                console.log(`[CONNECTED] ${clean}`);

                const name =
                    Primis.user?.name ||
                    Primis.user?.id?.split(':')[0] ||
                    clean;

                sessions.set(clean, {
                    sock: Primis,
                    store,
                    status: 'connected',
                    connectedAt: new Date().toISOString(),
                    name,
                    _retries: 0
                });

                pendingCodes.delete(clean);

                io.to(`numero:${clean}`).emit('session:connected', {
                    numero: clean,
                    name
                });

                io.emit('session:connected', {
                    numero: clean,
                    name
                });

                broadcastSessions();
            }

            if (connection === 'close') {
                const code =
                    lastDisconnect?.error?.output?.statusCode;

                const loggedOut =
                    code === DisconnectReason.loggedOut;

                console.log(
                    `[DISCONNECTED] ${clean} | Code: ${code}`
                );

                clearInterval(storeClearInterval);

                if (loggedOut) {
                    sessions.delete(clean);
                    pendingCodes.delete(clean);

                    fs.rmSync(getSessionDir(clean), {
                        recursive: true,
                        force: true
                    });

                    io.emit('session:disconnected', {
                        numero: clean,
                        reason: 'logged_out'
                    });

                    broadcastSessions();

                    return;
                }

                if (!Primis.authState.creds.registered) {
                    sessions.delete(clean);
                    pendingCodes.delete(clean);

                    io.to(`numero:${clean}`).emit('error', {
                        message: 'Connection closed before pairing completed. Please try again.'
                    });

                    broadcastSessions();

                    return;
                }

                const current = sessions.get(clean) || {};

                const retries = (current._retries || 0) + 1;

                if (retries >= 3) {
                    console.error(
                        `[RECONNECT] Maximum retries reached for ${clean}`
                    );

                    sessions.delete(clean);

                    io.emit('session:disconnected', {
                        numero: clean,
                        reason: 'unreachable'
                    });

                    broadcastSessions();

                    return;
                }

                try {
                    Primis.end(undefined);
                } catch (e) {
                    console.error('[SOCKET END]', e);
                }

                sessions.set(clean, {
                    ...current,
                    status: 'reconnecting',
                    _retries: retries
                });

                broadcastSessions();

                setTimeout(() => {
                    createSession(clean, null).catch(err => {
                        console.error('[RECONNECT ERROR]', err);
                    });
                }, 5000);
            }
        });

        Primis.ev.on('error', (err) => {
            console.error('[BAILEYS ERROR]', err);
        });

        Primis.ev.on('creds.update', saveCreds);

        sessions.set(clean, {
            ...sessions.get(clean),
            sock: Primis,
            store
        });

        return {
            success: true
        };

    } catch (err) {
        console.error('[CREATE SESSION ERROR]', err);

        clearInterval(storeClearInterval);

        sessions.delete(clean);
        pendingCodes.delete(clean);

        broadcastSessions();

        if (socketId) {
            io.to(socketId).emit('error', {
                message: 'Internal error, try again.'
            });
        }

        return {
            success: false,
            error: err?.message || 'create_session_error'
        };
    }
}

// IMPORTANT : configure WORKER_SECRET dans les variables
// d'environnement de ton hébergement.
const WORKER_SECRET = process.env.WORKER_SECRET;

app.get('/api/status', (req, res) => {
    const connectedCount = [...sessions.values()]
        .filter(s => s.status === 'connected').length;

    res.json({
        activeSessions: connectedCount,
        maxSessions: MAX_SESSIONS,
        available: connectedCount < MAX_SESSIONS
    });
});

app.post('/api/generate-pair', async (req, res) => {
    if (!WORKER_SECRET) {
        return res.status(503).json({
            success: false,
            error: 'worker_secret_not_configured'
        });
    }

    const secret = req.headers['x-worker-secret'];

    if (!secret || secret !== WORKER_SECRET) {
        return res.status(401).json({
            success: false,
            error: 'unauthorized'
        });
    }

    const { numero } = req.body || {};

    if (!numero) {
        return res.status(400).json({
            success: false,
            error: 'numero_required'
        });
    }

    try {
        const result = await createSession(numero, null);

        res.json(result || {
            success: false,
            error: 'unknown'
        });

    } catch (e) {
        console.error('[GENERATE PAIR]', e);

        res.status(500).json({
            success: false,
            error: 'server_error'
        });
    }
});

app.post('/api/disconnect', async (req, res) => {
    let { numero } = req.body || {};

    if (!numero) {
        return res.status(400).json({
            error: 'Number required'
        });
    }

    numero = cleanNumber(numero);

    const session = sessions.get(numero);

    if (session) {
        try {
            await session.sock?.logout?.();
        } catch (e) {
            console.error('[LOGOUT ERROR]', e);
        }

        sessions.delete(numero);
        pendingCodes.delete(numero);

        broadcastSessions();
    }

    try {
        fs.rmSync(getSessionDir(numero), {
            recursive: true,
            force: true
        });
    } catch (e) {
        console.error('[SESSION CLEANUP]', e);
    }

    res.json({
        ok: true,
        hadActiveSession: !!session
    });
});

app.get('/api/sessions', (req, res) => {
    const data = [];

    for (const [numero, s] of sessions.entries()) {
        data.push({
            numero,
            status: s.status,
            connectedAt: s.connectedAt,
            name: s.name || null
        });
    }

    res.json(data);
});

app.get('/api/code/:numero', (req, res) => {
    const numero = cleanNumber(req.params.numero);

    const code = pendingCodes.get(numero);
    const session = sessions.get(numero);

    res.json({
        code: code || null,
        status: session?.status || 'unknown'
    });
});

io.on('connection', (socket) => {
    const data = [];

    for (const [numero, s] of sessions.entries()) {
        data.push({
            numero,
            status: s.status,
            connectedAt: s.connectedAt,
            name: s.name
        });
    }

    socket.emit('sessions:update', data);

    socket.on('connect:numero', async ({ numero, rejoinOnly } = {}) => {
        if (!numero) return;

        const clean = cleanNumber(numero);

        if (!clean) return;

        socket.join(`numero:${clean}`);

        if (rejoinOnly) return;

        try {
            await createSession(clean, socket.id);
        } catch (e) {
            console.error('[SOCKET CONNECTION]', e);
        }
    });

    socket.on('disconnect', () => {
        console.log('[SOCKET] Client disconnected');
    });
});

server.listen(PORT, async () => {
    console.log(
        chalk.hex('#6c5ce7').bold(
            '\n╔══════════════════════════════════════╗'
        )
    );

    console.log(
        chalk.hex('#6c5ce7').bold(
            '║   Free Bug — Multi-Session          ║'
        )
    );

    console.log(
        chalk.hex('#6c5ce7').bold(
            `║   http://localhost:${PORT}             ║`
        )
    );

    console.log(
        chalk.hex('#6c5ce7').bold(
            '╚══════════════════════════════════════╝\n'
        )
    );

    try {
        await loadExistingSessions();
    } catch (e) {
        console.error('[LOAD SESSIONS ERROR]', e);
    }
});
