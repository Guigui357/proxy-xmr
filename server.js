const WebSocket = require("ws");
const net = require("net");

const PORT = process.env.PORT || 8080;

const POOL_HOST = "gulf.moneroocean.stream";
const POOL_PORT = 10128; // porta Stratum TCP

const server = new WebSocket.Server({
    port: PORT
});

console.log(`🚀 Proxy Stratum Node ativo na porta ${PORT}`);

server.on("connection", (ws) => {
    console.log("🔗 Cliente WebSocket conectado");

    let poolSocket = null;
    let buffer = "";

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
