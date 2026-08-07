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


console.log("🚀 Proxy iniciado");


wss.on("connection", (ws, req) => {

    console.log("\n🔗 WASM conectado");
    console.log("Origin:", req.headers.origin);


    let pool = null;
    let buffer = "";
    let loginData = null;



    function sendClient(data){

        if(ws.readyState === WebSocket.OPEN){

            const msg = JSON.stringify(data);

            console.log("📤 -> WASM:", msg);

            ws.send(msg);
        }

    }



    function connectPool(data){

        loginData = data;


        if(pool){

            pool.destroy();

        }


        console.log("🔌 Conectando pool...");


        pool = net.createConnection({

            host: POOL_HOST,
            port: POOL_PORT

        });



        pool.on("connect",()=>{


            console.log("✅ Pool conectada");


            const login = {

                id: data.id || 1,

                jsonrpc:"2.0",

                method:"login",

                params:{

                    login:
                    data.params?.login ||
                    data.login ||
                    "",


                    pass:
                    data.params?.pass ||
                    "x",


                    agent:
                    data.params?.agent ||
                    "MoneroMiner/1.0.0"

                }

            };


            console.log(
                "📤 -> POOL:",
                JSON.stringify(login)
            );


            pool.write(
                JSON.stringify(login)+"\n"
            );


        });




        pool.on("data",(chunk)=>{


            const raw = chunk.toString();


            console.log(
                "📥 POOL RAW:",
                raw
            );


            buffer += raw;


            const lines = buffer.split("\n");


            buffer = lines.pop();



            for(const line of lines){


                if(!line.trim())
                    continue;


                let msg;


                try{

                    msg = JSON.parse(line);

                }
                catch(e){

                    console.log(
                        "⚠ JSON inválido:",
                        line
                    );

                    continue;

                }



                console.log(
                    "📥 POOL JSON:",
                    msg
                );



                /*
                    LOGIN OK
                */

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


                    /*
                       Caso a pool mande job junto
                    */

                    if(msg.result.job){

                        sendClient({

                            jsonrpc:"2.0",

                            method:"job",

                            params:msg.result.job

                        });

                    }


                    continue;

                }



                /*
                    JOB
                */

                if(
                    msg.method === "job"
                ){

                    sendClient({

                        jsonrpc:"2.0",

                        method:"job",

                        params:msg.params

                    });


                    continue;

                }



                /*
                    SHARE RESULT
                */


                if(
                    msg.result ||
                    msg.error
                ){

                    sendClient(msg);

                    continue;

                }



                sendClient(msg);


            }


        });





        pool.on("error",(err)=>{

            console.log(
                "❌ Pool erro:",
                err.message
            );

        });



        pool.on("close",()=>{

            console.log(
                "🔌 Pool fechada"
            );

        });



    }






    ws.on("message",(raw)=>{


        const text = raw.toString();


        console.log(
            "\n📥 WASM:",
            text
        );


        let data;


        try{

            data = JSON.parse(text);

        }
        catch(e){

            console.log(
                "⚠ JSON WASM inválido"
            );

            return;

        }




        /*
            LOGIN
        */


        if(
            data.method === "login"
        ){

            connectPool(data);

            return;

        }





        /*
            SUBMIT SHARE
        */


        if(
            data.method === "submit"
        ){


            if(pool){

                console.log(
                    "📤 SHARE -> POOL"
                );


                pool.write(
                    JSON.stringify(data)+"\n"
                );


            }


            return;

        }



        console.log(
            "⚠ Método desconhecido:",
            data
        );



    });





    ws.on("close",()=>{


        console.log(
            "🔌 WASM desconectou"
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
            `🌐 WebSocket ativo na porta ${PORT}`
        );

    }
);
