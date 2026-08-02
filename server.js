const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

console.log(`🚀 Proxy Stratum-WebSocket ativo na porta ${PORT}`);

wss.on('connection', (ws) => {
    console.log('🔗 Navegador conectado ao proxy.');

    let stratumClient = null;
    let poolConnected = false;

    // Buffer para armazenar dados parciais recebidos da Pool TCP
    let tcpBuffer = "";

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📥 Recebido do navegador:', data.identifier || data.type || data);

            // 1. TRATAMENTO DO HANDSHAKE / LOGIN
            if (data.identifier === 'handshake' || data.type === 'login') {
                // Se já existir uma conexão ativa, fecha antes de abrir outra
                if (stratumClient) {
                    stratumClient.destroy();
                }

                stratumClient = new net.Socket();
                
                // Conexão TCP padrão na Pool da MoneroOcean (Porta Stratum 18081)
                const poolHost = data.pool || 'gulf.moneroocean.stream';
                const poolPort = 18081;

                stratumClient.connect(poolPort, poolHost, () => {
                    console.log(`✅ Conectado com sucesso via TCP à Pool: ${poolHost}:${poolPort}`);
                    poolConnected = true;

                    // Traduz o Handshake do navegador para o formato Stratum JSON-RPC exigido pela Pool
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

                // 2. RECEBE DADOS DA POOL TCP E TRADUZ PARA O NAVEGADOR
                stratumClient.on('data', (chunk) => {
                    tcpBuffer += chunk.toString();
                    
                    // O protocolo Stratum separa os pacotes obrigatoriamente por quebra de linha (\n)
                    let lines = tcpBuffer.split("\n");
                    tcpBuffer = lines.pop(); // Guarda pedaços incompletos no buffer

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        try {
                            const poolData = JSON.parse(line);
                            
                            // Traduz a resposta de Job/Trabalho da Pool para o formato que o seu miner.js entende
                            if (poolData.result && poolData.result.job) {
                                ws.send(JSON.stringify({
                                    identifier: "job",
                                    ...poolData.result.job
                                }));
                            } else if (poolData.method === "job") {
                                ws.send(JSON.stringify({
                                    identifier: "job",
                                    ...poolData.params
                                }));
                            } else if (poolData.result && poolData.result.status === "OK") {
                                // Confirmação de Hash aceito pela pool
                                ws.send(JSON.stringify({
                                    identifier: "hash",
                                    count: 1
                                }));
                            }
                        } catch (e) {
                            console.error("❌ Erro ao processar linha TCP:", e.message);
                        }
                    });
                });

                stratumClient.on('error', (err) => {
                    console.error('❌ Erro no socket TCP da Pool:', err.message);
                    ws.close();
                });

                stratumClient.on('close', () => {
                    console.log('❌ Conexão com a Pool encerrada.');
                    ws.close();
                });
            }

            // 3. TRATAMENTO DE ENVIO DE JOB RESOLVIDO (SUBMIT HASHES)
            if (data.identifier === 'submit' || data.type === 'submit') {
                if (poolConnected && stratumClient && stratumClient.writable) {
                    const stratumSubmit = {
                        id: 2,
                        method: "submit",
                        params: {
                            id: data.job_id,
                            job_id: data.job_id,
                            nonce: data.nonce,
                            result: data.result
                        }
                    };
                    stratumClient.write(JSON.stringify(stratumSubmit) + "\n");
                }
            }

        } catch (err) {
            console.error('❌ Erro ao parsear mensagem do WebSocket:', err.message);
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
