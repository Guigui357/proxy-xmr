const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const cors = require('cors');

const PORT = process.env.PORT || 8080;

const serverHttp = http.createServer((req, res) => {
    cors()(req, res, () => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Proxy CryptoNight Ativo e Live!');
    });
});

const wss = new WebSocket.Server({ server: serverHttp });
console.log(`🚀 Proxy Stratum ativo na porta ${PORT}`);

wss.on('connection', (ws) => {
    console.log('🔗 SINAL RECEBIDO: O navegador conectou com o Proxy!');
    let stratumClient = null;
    let poolConnected = false;
    let tcpBuffer = "";

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📥 Mensagem do Navegador:', data.identifier || data.type || 'Dados crus');

            // Validação flexível para capturar o Handshake enviado pelo miner.js
            if (data.identifier === 'handshake' || data.type === 'login' || data.identifier === 'login') {
                if (stratumClient) stratumClient.destroy();
                stratumClient = new net.Socket();
                
                console.log('⏳ Tentando abrir socket TCP com a Pool MoneroOcean...');
                
                stratumClient.connect(10001, 'gulf.moneroocean.stream', () => {
                    console.log('✅ CONECTADO VIA TCP À POOL MONEROOCEAN!');
                    poolConnected = true;

                    // Captura a carteira enviada ou usa a padrão de contingência
                    let userWallet = data.login || data.wallet || "4657q4dnsjLWtzeW4XN3wG9swFumWAZB9i1pegTLMxVAQy5E5AE8uif42kkHWcWc9vDcLUmzeCf3pV7mmrJQQqqe84dtASi";
                    
                    // CORREÇÃO: Formata a carteira adicionando o worker e o sufixo separadamente
                    if (!userWallet.includes('~cn/lite')) {
                        if (!userWallet.includes('.')) {
                            userWallet += ".webMiner~cn/lite";
                        } else {
                            userWallet += "~cn/lite";
                        }
                    }

                    // Monta o cabeçalho perfeito aceito pela MoneroOcean
                    const stratumLogin = {
                        id: 1,
                        method: "login",
                        params: {
                            login: userWallet,
                            pass: "~cn/lite",
                            agent: "XMR-CryptoNightWeb/1.0",
                            algo: ["cn/lite"]
                        }
                    };
                    
                    stratumClient.write(JSON.stringify(stratumLogin) + "\n");
                    console.log('📤 Login enviado para a Pool:', userWallet);
                });

                stratumClient.on('data', (chunk) => {
                    tcpBuffer += chunk.toString();
                    
                    let lines = tcpBuffer.split("\n");
                    tcpBuffer = lines.pop(); // Guarda a linha incompleta se houver

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        try {
                            console.log("📥 Linha Processada da Pool:", line);
                            const poolData = JSON.parse(line);
                            
                            // Trata os erros enviados pela Pool para não fechar às cegas
                            if (poolData.error) {
                                console.error(`❌ Erro retornado pela pool: [${poolData.error.code}] ${poolData.error.message}`);
                                ws.send(JSON.stringify({ identifier: "error", message: poolData.error.message }));
                                return;
                            }

                            // Encaminha os Jobs reais para o miner.js
                            if (poolData.result && poolData.result.job) {
                                ws.send(JSON.stringify({ identifier: "job", ...poolData.result.job }));
                            } else if (poolData.method === "job") {
                                ws.send(JSON.stringify({ identifier: "job", ...poolData.params }));
                            } else if (poolData.result && poolData.result.status === "OK") {
                                ws.send(JSON.stringify({ identifier: "hash" }));
                                console.log("🔥 SUCESSO: Hash validado e aceito!");
                            }
                        } catch (e) {
                            console.error("❌ Falha ao parsear JSON da Pool:", e.message);
                        }
                    });
                });

                stratumClient.on('error', (err) => console.error('❌ Erro no socket TCP:', err.message));
                stratumClient.on('close', () => {
                    console.log('❌ Conexão com a Pool encerrada.');
                    poolConnected = false;
                });
            }

            if (data.identifier === 'submit' && poolConnected && stratumClient?.writable) {
                const stratumSubmit = {
                    id: 2,
                    method: "submit",
                    params: { 
                        id: data.job_id, 
                        job_id: data.job_id, 
                        nonce: data.nonce, 
                        result: data.result,
                        algo: "cn/lite" // Informa qual algoritmo produziu o hash enviado
                    }
                };
                stratumClient.write(JSON.stringify(stratumSubmit) + "\n");
                console.log(`📤 Compartilhamento (Share) enviado para a Pool.`);
            }
        } catch (err) {
            console.error('❌ Erro no processamento do WebSocket:', err.message);
        }
    });

    ws.on('close', () => { if (stratumClient) stratumClient.destroy(); });
});

serverHttp.listen(PORT);
