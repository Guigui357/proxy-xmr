const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const cors = require('cors');

const PORT = process.env.PORT || 8080;

// Cria um servidor HTTP nativo para aplicar as regras de segurança CORS
const serverHttp = http.createServer((req, res) => {
    cors()(req, res, () => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Proxy Stratum Ativo e Live!');
    });
});

// Acopla o servidor WebSocket dentro do servidor HTTP protegido
const wss = new WebSocket.Server({ server: serverHttp });

console.log(`🚀 Proxy com liberação CORS ativo na porta ${PORT}`);

wss.on('connection', (ws, req) => {
    console.log('🔗 SINAL RECEBIDO: Navegador conectado com sucesso ao proxy!');

    let stratumClient = null;
    let poolConnected = false;
    let tcpBuffer = "";

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📥 Mensagem decodificada:', data.identifier || data.type || data);

            if (data.identifier === 'handshake' || data.type === 'login') {
                if (stratumClient) stratumClient.destroy();

                stratumClient = new net.Socket();
                const poolHost = data.pool || 'gulf.moneroocean.stream';
                const poolPort = 18081;

                stratumClient.connect(poolPort, poolHost, () => {
                    console.log(`✅ Proxy conectado via TCP à Pool: ${poolHost}:${poolPort}`);
                    poolConnected = true;

                    const stratumLogin = {
                        id: 1,
                        method: "login",
                        params: {
                            login: data.login || data.wallet || "4657q4dnsjLWtzeW4XN3wG9swFumWAZB9i1pegTLMxVAQy5E5AE8uif42kkHWcWc9vDcLUmzeCf3pV7mmrJQQqqe84dtASi",
                            pass: data.password || "x",
                            agent: "XMR-WebMiner-Proxy/1.0"
                        }
                    };
                    stratumClient.write(JSON.stringify(stratumLogin) + "\n");
                });

                stratumClient.on('data', (chunk) => {
                    tcpBuffer += chunk.toString();
                    let lines = tcpBuffer.split("\n");
                    tcpBuffer = lines.pop();

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        try {
                            const poolData = JSON.parse(line);
                            if (poolData.result && poolData.result.job) {
                                ws.send(JSON.stringify({ identifier: "job", ...poolData.result.job }));
                            } else if (poolData.method === "job") {
                                ws.send(JSON.stringify({ identifier: "job", ...poolData.params }));
                            } else if (poolData.result && poolData.result.status === "OK") {
                                ws.send(JSON.stringify({ identifier: "hash", count: 1 }));
                            }
                        } catch (e) {
                            console.error("❌ Erro ao parsear linha TCP:", e.message);
                        }
                    });
                });

                stratumClient.on('error', (err) => {
                    console.error('❌ Erro no socket TCP:', err.message);
                    ws.close();
                });

                stratumClient.on('close', () => {
                    console.log('❌ Conexão com a Pool encerrada.');
                    ws.close();
                });
            }

            if (data.identifier === 'submit' || data.type === 'submit') {
                if (poolConnected && stratumClient && stratumClient.writable) {
                    const stratumSubmit = {
                        id: 2,
                        method: "submit",
                        params: { id: data.job_id, job_id: data.job_id, nonce: data.nonce, result: data.result }
                    };
                    stratumClient.write(JSON.stringify(stratumSubmit) + "\n");
                }
            }

        } catch (err) {
            console.error('❌ Erro no processamento do WebSocket:', err.message);
        }
    });

    ws.on('close', () => {
        console.log('🔌 Navegador desconectado do proxy.');
        if (stratumClient) stratumClient.destroy();
    });

    ws.on('error', (err) => {
        if (stratumClient) stratumClient.destroy();
    });
});

// Faz o servidor escutar a porta do Render
serverHttp.listen(PORT, () => {
    console.log(`🌍 Servidor HTTP/WS escutando requisições na porta ${PORT}`);
});
