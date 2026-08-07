const WebSocket = require("ws");
const net = require("net");
const http = require("node:http")
const PORT = process.env.PORT || 8080;

const POOL_HOST = "gulf.moneroocean.stream";
const POOL_PORT = 10128; // porta Stratum TCP

const httpServer = http.createServer((req, res) => {

    res.writeHead(200);

    res.end("Proxy online");

});

const wss = new WebSocket.Server({

    server: httpServer

});

process.on("uncaughtException", (err) => {
    console.log("❌ EXCEPTION:", err);
});

process.on("unhandledRejection", (err) => {
    console.log("❌ REJECTION:", err);
});

console.log(`🚀 Proxy Stratum Node ativo na porta ${PORT}`);

wss.on("connection", (ws, req) => {
    console.log("🔗 Cliente WebSocket conectado");

    let poolSocket = null;
    let buffer = "";

    console.log("🔗 WebSocket conectado:", req.headers);
    ws.on("message", (msg)=>{
        console.log("RAW CLIENTE:", msg.toString());
    });

    ws.on("message", (msg) => {
        try {
            const data = JSON.parse(msg.toString());

            console.log("📥 Cliente:", data);

            // Login do minerador
            if (
                data.method === "login" ||
                data.identifier === "handshake" ||
                data.identifier === "login"
            ) {

                if (poolSocket) {
                    poolSocket.destroy();
                }

                poolSocket = net.createConnection({
                    host: POOL_HOST,
                    port: POOL_PORT
                });

                poolSocket.on("connect", () => {
                    console.log("✅ Conectado na Pool");

                    const login = {
                        id: data.id || 1,
                        method: "login",
                        params: {
                            login:
                                data.params?.login ||
                                data.wallet,
                            pass:
                                data.params?.pass ||
                                "worker",
                            agent: "WebMiner/1.0"
                        }
                    };

                    poolSocket.write(
                        JSON.stringify(login) + "\n"
                    );

                    console.log("📤 Login enviado");
                });


                poolSocket.on("data", (chunk) => {

                    buffer += chunk.toString();

                    let lines = buffer.split("\n");
                    buffer = lines.pop();

                    for (const line of lines) {

                        if (!line.trim())
                            continue;

                        console.log("📥 Pool:", line);

                        try {
                            const poolData =
                                JSON.parse(line);


                            // Login OK
                            if (
                                poolData.result &&
                                poolData.result.status === "OK"
                            ) {

                                ws.send(JSON.stringify({
                                    identifier:
                                        "handshake_reply",
                                    status:
                                        "authenticated"
                                }));

                                continue;
                            }


                            // Job
                            if (
                                poolData.result?.job
                            ) {

                                ws.send(JSON.stringify({
                                    identifier:"job",
                                    ...poolData.result.job
                                }));

                                console.log(
                                    "🎯 Job enviado"
                                );

                                continue;
                            }


                            ws.send(
                                JSON.stringify(poolData)
                            );


                        } catch(e){
                            console.log(
                                "JSON inválido da pool"
                            );
                        }
                    }
                });


                poolSocket.on("error", (err)=>{
                    console.log(
                        "❌ Erro Pool:",
                        err.message
                    );
                });


                poolSocket.on("close", ()=>{
                    console.log(
                        "🔌 Pool fechada"
                    );
                });
            }


            // Submit de share
            else if (
                data.method === "submit" ||
                data.identifier === "submit"
            ){

                if(poolSocket){

                    poolSocket.write(
                        JSON.stringify(data)
                        + "\n"
                    );

                    console.log(
                        "📤 Share enviado"
                    );
                }
            }


        } catch(err){

            console.log(
                "JSON cliente inválido:",
                err.message
            );

        }
    });


    ws.on("close",()=>{

        console.log(
            "🔌 Cliente saiu"
        );

        if(poolSocket)
            poolSocket.destroy();

    });

});

httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Servidor ouvindo na porta ${PORT}`);
});
