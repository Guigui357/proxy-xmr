const WebSocket = require("ws");
const net = require("net");
const http = require("http");

const PORT = process.env.PORT || 8080;

const POOL_HOST = "gulf.moneroocean.stream";
const POOL_PORT = 10128;

const httpServer = http.createServer((req, res) => {
    res.writeHead(200, {
        "Content-Type": "text/plain"
    });

    res.end("XMR WebSocket Proxy online");
});


const wss = new WebSocket.Server({
    server: httpServer
});


console.log(`🚀 Proxy iniciado na porta ${PORT}`);


wss.on("connection", (ws, req) => {

    console.log("\n🔗 Cliente WebSocket conectado");
    console.log("Origin:", req.headers.origin);


    let pool = null;
    let poolBuffer = "";


    function sendClient(obj){

        if(ws.readyState === WebSocket.OPEN){

            ws.send(JSON.stringify(obj));

            console.log("📤 WASM <-", obj);

        }

    }


    function connectPool(loginData){

        if(pool){

            pool.destroy();
            pool = null;

        }


        console.log("🔌 Conectando na pool...");


        pool = net.createConnection({

            host: POOL_HOST,
            port: POOL_PORT

        });


        pool.on("connect",()=>{

            console.log("✅ Pool conectada");


            const login = {

                id: loginData.id || 1,

                jsonrpc:"2.0",

                method:"login",

                params:{

                    login:
                        loginData.params?.login ||
                        loginData.login ||
                        "",

                    pass:
                        loginData.params?.pass ||
                        "x",

                    agent:
                        "MoneroWebMiner/1.0"

                }

            };


            console.log("📤 Pool <-", login);


            pool.write(
                JSON.stringify(login)+"\n"
            );


        });



        pool.on("data",(chunk)=>{


            poolBuffer += chunk.toString();


            const lines = poolBuffer.split("\n");

            poolBuffer = lines.pop();



            for(const line of lines){


                if(!line.trim())
                    continue;


                console.log("📥 Pool ->", line);



                try{

                    const msg = JSON.parse(line);



                    // Login aceito

                    if(
                        msg.result &&
                        msg.result.status === "OK"
                    ){

                        sendClient({

                            id:msg.id || 1,

                            jsonrpc:"2.0",

                            result:{
                                status:"OK"
                            }

                        });


                        continue;

                    }



                    // Job recebido

                    if(
                        msg.method === "job" &&
                        msg.params
                    ){

                        sendClient({

                            method:"job",

                            params:msg.params

                        });


                        continue;

                    }



                    // outras mensagens

                    sendClient(msg);



                }catch(e){

                    console.log(
                        "⚠️ JSON pool inválido"
                    );

                }

            }


        });



        pool.on("error",(err)=>{

            console.log(
                "❌ Erro pool:",
                err.message
            );

        });



        pool.on("close",()=>{

            console.log(
                "🔌 Pool desconectada"
            );

        });


    }



    ws.on("message",(raw)=>{


        console.log(
            "\n📥 WASM ->",
            raw.toString()
        );


        let data;


        try{

            data = JSON.parse(
                raw.toString()
            );

        }catch(e){

            console.log(
                "Mensagem inválida"
            );

            return;

        }



        /*
            LOGIN
        */

        if(
            data.method === "login" ||
            data.identifier === "login" ||
            data.identifier === "handshake"
        ){

            connectPool(data);

            return;

        }



        /*
            SUBMIT SHARE
        */

        if(
            data.method === "submit" ||
            data.identifier === "submit"
        ){


            if(pool){

                pool.write(
                    JSON.stringify(data)+"\n"
                );


                console.log(
                    "📤 Share enviado"
                );

            }


            return;

        }



        console.log(
            "⚠️ Método desconhecido:",
            data
        );


    });



    ws.on("close",()=>{


        console.log(
            "🔌 Cliente saiu"
        );


        if(pool){

            pool.destroy();

        }


    });


});



httpServer.listen(
    PORT,
    "0.0.0.0",
    ()=>{

        console.log(
            `🌐 Escutando ${PORT}`
        );

    }
);
