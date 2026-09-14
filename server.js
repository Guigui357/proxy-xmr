const WebSocket = require("ws");
const net = require("net");
const http = require("http");

const PORT = process.env.PORT || 8080;

// XMRPool.eu
const POOL_HOST = "xmrpool.eu";
const POOL_PORT = 5555;

const httpServer = http.createServer((req, res) => {
    res.writeHead(200, {
        "Content-Type": "text/plain"
    });

    res.end("XMR WebSocket Proxy online");
});

const wss = new WebSocket.Server({
    server: httpServer
});

console.log("🚀 Proxy iniciado");
console.log(`⛏️ Pool: ${POOL_HOST}:${POOL_PORT}`);

wss.on("connection", (ws, req) => {

    console.log("\n🔗 WASM conectado");
    console.log("Origin:", req.headers.origin);

    let pool = null;
    let poolBuffer = "";
    let loginData = null;

    function sendClient(data) {

        if (ws.readyState !== WebSocket.OPEN)
            return;

        const msg = JSON.stringify(data);

        console.log("📤 -> WASM:", msg);

        ws.send(msg);
    }

    function connectPool(data) {

        loginData = data;

        if (pool) {
            pool.destroy();
            pool = null;
        }

        /*
         * O minerador deve mandar:
         *
         * wallet + worker
         *
         * Exemplo:
         *
         * 4xxxxxxxxxxxxxxxx+iphone_13
         *
         * NÃO transformar em wallet.worker.
         * NÃO separar o worker.
         */

        const login =
            data.params?.login ||
            data.login ||
            "";

        const pass =
            data.params?.pass ||
            "x";

        const agent =
            data.params?.agent ||
            "MoneroMiner/1.0.0";

        console.log("\n🔐 LOGIN recebido do WASM:");
        console.log("   login:", login);
        console.log("   pass:", pass);
        console.log("   agent:", agent);

        /*
         * Validação simples.
         *
         * Para XMRPool.eu esperamos:
         *
         * WALLET+WORKER
         */

        if (!login) {

            console.log("❌ Login vazio");

            sendClient({
                id: data.id || 1,
                jsonrpc: "2.0",
                error: {
                    code: -1,
                    message: "Wallet/login vazio"
                }
            });

            return;
        }

        if (!login.includes("+")) {

            console.log(
                "⚠️ AVISO: login não contém '+worker'"
            );

            console.log(
                "   Esperado: WALLET+WORKER"
            );

        }

        console.log(
            "📌 Login final para XMRPool.eu:",
            login
        );

        console.log(
            "🔌 Conectando em XMRPool.eu..."
        );

        pool = net.createConnection({
            host: POOL_HOST,
            port: POOL_PORT
        });

        pool.on("connect", () => {

            console.log(
                "✅ XMRPool.eu conectada"
            );

            /*
             * IMPORTANTE:
             *
             * O login é encaminhado exatamente
             * como recebido.
             *
             * Exemplo:
             *
             * 4xxxxx+iphone_13
             */

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

        /*
         * Dados recebidos da pool
         */

        pool.on("data", (chunk) => {

            poolBuffer += chunk.toString();

            const lines = poolBuffer.split("\n");

            poolBuffer = lines.pop();

            for (const line of lines) {

                if (!line.trim())
                    continue;

                let msg;

                try {

                    msg = JSON.parse(line);

                }
                catch (e) {

                    console.log(
                        "⚠️ JSON inválido da pool:",
                        line
                    );

                    continue;

                }

                console.log(
                    "📥 XMRPool.eu:",
                    msg
                );

                /*
                 * LOGIN OK
                 */

                if (
                    msg.result &&
                    msg.result.status === "OK"
                ) {

                    sendClient({

                        id: msg.id || 1,

                        jsonrpc: "2.0",

                        result: {
                            status: "OK"
                        }

                    });

                    /*
                     * Algumas pools podem mandar
                     * o primeiro job junto com login.
                     */

                    if (msg.result.job) {

                        sendClient({

                            jsonrpc: "2.0",

                            method: "job",

                            params: msg.result.job

                        });

                    }

                    continue;
                }

                /*
                 * JOB
                 */

                if (msg.method === "job") {

                    sendClient({

                        jsonrpc: "2.0",

                        method: "job",

                        params: msg.params

                    });

                    continue;
                }

                /*
                 * RESULTADO DO SHARE
                 */

                if (
                    msg.result !== undefined ||
                    msg.error !== undefined
                ) {

                    sendClient(msg);

                    continue;
                }

                /*
                 * Qualquer outra mensagem
                 */

                sendClient(msg);
            }

        });

        pool.on("error", (err) => {

            console.log(
                "❌ XMRPool.eu erro:",
                err.message
            );

        });

        pool.on("close", () => {

            console.log(
                "🔌 XMRPool.eu conexão fechada"
            );

            pool = null;

        });

    }

    /*
     * Mensagens do WASM
     */

    ws.on("message", (raw) => {

        const text = raw.toString();

        console.log(
            "\n📥 WASM:",
            text
        );

        let data;

        try {

            data = JSON.parse(text);

        }
        catch (e) {

            console.log(
                "⚠️ JSON WASM inválido"
            );

            return;
        }

        /*
         * LOGIN
         */

        if (data.method === "login") {

            connectPool(data);

            return;
        }

        /*
         * SUBMIT SHARE
         */

        if (data.method === "submit") {

            if (!pool) {

                console.log(
                    "⚠️ Share recebido sem pool"
                );

                return;
            }

            console.log(
                "📤 SHARE -> XMRPool.eu"
            );

            pool.write(
                JSON.stringify(data) + "\n"
            );

            return;
        }

        /*
         * PING
         */

        if (data.method === "keepalive") {

            if (pool) {

                pool.write(
                    JSON.stringify(data) + "\n"
                );

            }

            return;
        }

        console.log(
            "⚠️ Método desconhecido:",
            data
        );

    });

    ws.on("close", () => {

        console.log(
            "🔌 WASM desconectou"
        );

        if (pool) {

            pool.destroy();
            pool = null;

        }

    });

    ws.on("error", (err) => {

        console.log(
            "❌ WebSocket erro:",
            err.message
        );

    });

});

httpServer.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `🌐 WebSocket ativo na porta ${PORT}`
        );

    }
);
