const WebSocket = require("ws");
const net = require("net");
const http = require("http");

const PORT = process.env.PORT || 8080;

const POOL_HOST = "xmrpool.eu";
const POOL_PORT = 5555;

// ============================================================
// HTTP
// ============================================================

const httpServer = http.createServer((req, res) => {
    res.writeHead(200, {
        "Content-Type": "text/plain"
    });

    res.end("XMR WebSocket Proxy online");
});

// ============================================================
// WEBSOCKET
// ============================================================

const wss = new WebSocket.Server({
    server: httpServer
});

console.log("🚀 Proxy iniciado");
console.log(`🎯 Pool: ${POOL_HOST}:${POOL_PORT}`);

wss.on("connection", (ws, req) => {

    console.log("\n🔗 WASM conectado");
    console.log("Origin:", req.headers.origin);

    let pool = null;
    let buffer = "";

    // ========================================================
    // Envia JSON para o WASM
    // ========================================================

    function sendClient(data) {

        if (ws.readyState !== WebSocket.OPEN) {
            console.log("⚠ WASM não está conectado");
            return;
        }

        const msg = JSON.stringify(data);

        console.log("📤 -> WASM:", msg);

        ws.send(msg);
    }

    // ========================================================
    // Conecta no XMRPool
    // ========================================================

    function connectPool(data) {

        if (pool) {
            console.log("♻ Fechando conexão anterior com pool...");
            pool.destroy();
            pool = null;
        }

        buffer = "";

        const login =
            data.params?.login ||
            data.login ||
            "";

        const pass =
            data.params?.pass ||
            data.pass ||
            "x";

        const agent =
            data.params?.agent ||
            data.agent ||
            "MoneroMiner/1.0.0";

        console.log("\n🔐 LOGIN recebido do WASM:");
        console.log("   login:", login);
        console.log("   pass:", pass);
        console.log("   agent:", agent);

        console.log(
            "📌 Login final para XMRPool.eu:",
            login
        );

        console.log("🔌 Conectando em XMRPool.eu...");

        pool = net.createConnection({
            host: POOL_HOST,
            port: POOL_PORT
        });

        // ====================================================
        // POOL CONNECTED
        // ====================================================

        pool.on("connect", () => {

            console.log("✅ XMRPool.eu conectada");

            const loginRequest = {
                id: data.id || 1,
                jsonrpc: "2.0",
                method: "login",
                params: {
                    login: login,
                    pass: pass,
                    agent: agent
                }
            };

            const payload =
                JSON.stringify(loginRequest) + "\n";

            console.log(
                "📤 -> XMRPool.eu:",
                JSON.stringify(loginRequest)
            );

            pool.write(payload);
        });

        // ====================================================
        // POOL DATA
        // ====================================================

        pool.on("data", (chunk) => {

            const raw = chunk.toString();

            console.log(
                "📥 XMRPool.eu RAW:",
                raw
            );

            buffer += raw;

            const lines = buffer.split("\n");

            buffer = lines.pop();

            for (const line of lines) {

                if (!line.trim()) {
                    continue;
                }

                let msg;

                try {

                    msg = JSON.parse(line);

                } catch (e) {

                    console.log(
                        "⚠ JSON inválido recebido da pool:",
                        line
                    );

                    continue;
                }

                console.log(
                    "📥 XMRPool.eu:",
                    msg
                );

                // =================================================
                // LOGIN RESPONSE
                //
                // IMPORTANTE:
                // Preserva result.id, que é o SESSION ID.
                // =================================================

                if (
                    msg.result &&
                    msg.result.status === "OK"
                ) {

                    console.log(
                        "✅ LOGIN ACEITO PELO XMRPool.eu"
                    );

                    if (msg.result.id !== undefined) {

                        console.log(
                            "🆔 SESSION ID:",
                            msg.result.id
                        );
                    }

                    // Envia o RESULT COMPLETO para o WASM.
                    //
                    // NÃO fazer:
                    // result: { status: "OK" }
                    //
                    // porque isso apagaria result.id.

                    sendClient({
                        id: msg.id || 1,
                        jsonrpc: "2.0",
                        result: msg.result
                    });

                    // =================================================
                    // JOB INICIAL
                    // =================================================

                    if (msg.result.job) {

                        console.log(
                            "📋 Enviando JOB inicial para WASM"
                        );

                        sendClient({
                            jsonrpc: "2.0",
                            method: "job",
                            params: msg.result.job
                        });
                    }

                    continue;
                }

                // =================================================
                // NOVO JOB
                // =================================================

                if (msg.method === "job") {

                    console.log(
                        "📋 Novo JOB recebido da pool:",
                        msg.params?.job_id
                    );

                    sendClient({
                        jsonrpc: "2.0",
                        method: "job",
                        params: msg.params
                    });

                    continue;
                }

                // =================================================
                // SUBMIT RESPONSE / ERROR
                // =================================================

                if (
                    msg.result !== undefined ||
                    msg.error !== undefined
                ) {

                    if (msg.error) {

                        console.log(
                            "❌ POOL REJEITOU:",
                            msg.error
                        );

                    } else {

                        console.log(
                            "✅ POOL RESPONDEU:",
                            msg.result
                        );
                    }

                    sendClient(msg);

                    continue;
                }

                // =================================================
                // QUALQUER OUTRA MENSAGEM
                // =================================================

                sendClient(msg);
            }
        });

        // ====================================================
        // POOL ERROR
        // ====================================================

        pool.on("error", (err) => {

            console.log(
                "❌ XMRPool.eu erro:",
                err.message
            );
        });

        // ====================================================
        // POOL CLOSE
        // ====================================================

        pool.on("close", () => {

            console.log(
                "🔌 XMRPool.eu conexão fechada"
            );
        });
    }

    // ========================================================
    // MENSAGENS DO WASM
    // ========================================================

    ws.on("message", (raw) => {

        const text = raw.toString();

        console.log(
            "\n📥 WASM:",
            text
        );

        let data;

        try {

            data = JSON.parse(text);

        } catch (e) {

            console.log(
                "⚠ JSON WASM inválido"
            );

            return;
        }

        // ====================================================
        // LOGIN
        // ====================================================

        if (data.method === "login") {

            connectPool(data);

            return;
        }

        // ====================================================
        // SUBMIT SHARE
        // ====================================================

        if (data.method === "submit") {

            if (!pool) {

                console.log(
                    "❌ SHARE recebido mas pool não está conectada"
                );

                sendClient({
                    id: data.id || 1,
                    jsonrpc: "2.0",
                    error: {
                        code: -1,
                        message: "Pool not connected"
                    }
                });

                return;
            }

            console.log(
                "📤 SHARE -> XMRPool.eu:",
                JSON.stringify(data)
            );

            pool.write(
                JSON.stringify(data) + "\n"
            );

            return;
        }

        // ====================================================
        // MÉTODO DESCONHECIDO
        // ====================================================

        console.log(
            "⚠ Método desconhecido:",
            data.method
        );
    });

    // ========================================================
    // WASM CLOSE
    // ========================================================

    ws.on("close", () => {

        console.log(
            "🔌 WASM desconectou"
        );

        if (pool) {

            pool.destroy();
            pool = null;
        }
    });

    // ========================================================
    // WASM ERROR
    // ========================================================

    ws.on("error", (err) => {

        console.log(
            "❌ WebSocket WASM erro:",
            err.message
        );

        if (pool) {

            pool.destroy();
            pool = null;
        }
    });
});

// ============================================================
// HTTP SERVER
// ============================================================

httpServer.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `🌐 WebSocket ativo na porta ${PORT}`
        );

        console.log(
            `🎯 XMRPool.eu: ${POOL_HOST}:${POOL_PORT}`
        );
    }
);
