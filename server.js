const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const cors = require('cors');

const PORT = process.env.PORT || 8080;

const serverHttp = http.createServer((req, res) => {
    cors()(req, res, () => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Proxy CryptoNight Ativo!');
    });
});

const wss = new WebSocket.Server({ server: serverHttp });
console.log(`🚀 Proxy Stratum ativo na porta ${PORT}`);

wss.on('connection', (ws) => {
    console.log('🔗 SINAL RECEBIDO: Navegador conectado ao Proxy.');
    let stratumClient = null;
    let poolConnected = false;
    let tcpBuffer = "";

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (data.identifier === 'handshake') {
                if (stratumClient) stratumClient.destroy();
                stratumClient = new net.Socket();
                
                // Conectando na porta alternativa adaptada para algoritmos de CPU leve na MoneroOcean
                stratumClient.connect(18081, 'gulf.moneroocean.stream', () => {
                    console.log('✅ Proxy conectado via TCP à Pool MoneroOcean (CN-Lite Protocol)');
                    poolConnected = true;

                    const stratumLogin = {
                        id: 1,
                        method: "login",
                        params: {
                            login: data.login,
                            pass: "cn-lite",
                            agent: "XMR-CryptoNightWeb/1.0"
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
                            // Envia os Jobs criptográficos reais da Pool direto para o miner.js
                            if (poolData.result && poolData.result.job) {
                                ws.send(JSON.stringify({ identifier: "job", ...poolData.result.job }));
                            } else if (poolData.method === "job") {
                                ws.send(JSON.stringify({ identifier: "job", ...poolData.params }));
                            } else if (poolData.result && poolData.result.status === "OK") {
                                ws.send(JSON.stringify({ identifier: "hash" }));
                                console.log("🔥 SUCESSO: Hash aceito e computado na Pool MoneroOcean!");
                            }
                        } catch (e) {}
                    });
                });

                stratumClient.on('error', () => ws.close());
                stratumClient.on('close', () => ws.close());
            }

            // Repassa os hashes reais minerados para a validação da Pool
            if (data.identifier === 'submit' && poolConnected && stratumClient?.writable) {
                const stratumSubmit = {
                    id: 2,
                    method: "submit",
                    params: { id: data.job_id, job_id: data.job_id, nonce: data.nonce, result: data.result }
                };
                stratumClient.write(JSON.stringify(stratumSubmit) + "\n");
                console.log(`📤 Enviando share calculado para a Pool (Job ID: ${data.job_id})`);
            }
        } catch (err) {}
    });

    ws.on('close', () => { if (stratumClient) stratumClient.destroy(); });
});

serverHttp.listen(PORT);
