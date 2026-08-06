const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const cors = require('cors');

const PORT = process.env.PORT || 8080;

// 1. CRIAÇÃO DO SERVIDOR HTTP COM SUPORTE A CORS
const serverHttp = http.createServer((req, res) => {
    cors()(req, res, () => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Proxy Monero Ativo e Live!');
    });
});

// 2. CONFIGURAÇÃO DO SERVIDOR WEBSOCKET COM TRATAMENTO DE SUB-PROTOCOLOS
const wss = new WebSocket.Server({ 
    noServer: true, 
    handleProtocols: (protocols) => {
        return protocols.size > 0 ? Array.from(protocols) : false;
    }
});

// 3. CAPTURA DE CONEXÃO DIRETA SEM RISCO DE ERRO DE URL NO RENDER
serverHttp.on('upgrade', (request, socket, head) => {
    try {
        // Ignora checagens complexas de string de URL para evitar o crash síncrono no Render
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    } catch (err) {
        console.error('❌ Falha fatal no upgrade do WebSocket:', err.message);
        socket.destroy();
    }
});

console.log(`🚀 Proxy Stratum ativo na porta ${PORT}`);

// 4. ORQUESTRADOR DE EVENTOS DE CONEXÃO
wss.on('connection', (ws) => {
    console.log('🔗 SINAL RECEBIDO: O navegador conectou com o Proxy!');
    
    let stratumClient = null;
    let poolConnected = false;
    let tcpBuffer = "";
    let lastClientRpcId = 1;

    // Listener de segurança para o próprio WebSocket
    ws.on('error', (err) => console.error('❌ Erro no canal WebSocket do navegador:', err.message));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📥 Mensagem do Navegador:', data.method || data.identifier || 'Dados crus');

            if (data.id) lastClientRpcId = data.id;

            // CAPTURA DE LOGIN / HANDSHAKE DINÂMICO
            if (data.method === 'login' || data.identifier === 'handshake' || data.identifier === 'login') {
                if (stratumClient) stratumClient.destroy();
                
                stratumClient = new net.Socket();
                
                // Mapeia chaves de contingência vindas do C++ ou HTML
                const params = data.params || {};
                const userWallet = params.login || data.wallet || "4657q4dnsjLWtzeW4XN3wG9swFumWAZB9i1pegTLMxVAQy5E5AE8uif42kkHWcWc9vDcLUmzeCf3pV7mmrJQQqqe84dtASi";
                const workerName = params.pass || data.worker || "WasmMiner";

                console.log(`⏳ [PROCESSO] Iniciando canal TCP. Carteira: ${userWallet.substring(0, 12)}...`);
                
                // Conecta na porta web padrão 443 para mascarar o tráfego no firewall do Render
                stratumClient.connect(443, 'gulf.moneroocean.stream', () => {
                    console.log('✅ CONECTADO VIA TCP À POOL MONEROOCEAN!');
                    poolConnected = true;

                    const stratumLogin = {
                        id: lastClientRpcId,
                        method: "login",
                        params: {
                            login: userWallet,
                            pass: workerName,
                            agent: "XMR-CryptoNightWeb/1.0"
                        }
                    };
                    
                    stratumClient.write(JSON.stringify(stratumLogin) + "\n");
                    console.log('📤 Login Stratum enviado para a Pool.');
                });

                // TRATAMENTO DE RETORNO DA POOL -> PROXY -> C++
                stratumClient.on('data', (chunk) => {
                    tcpBuffer += chunk.toString();
                    let lines = tcpBuffer.split("\n");
                    tcpBuffer = lines.pop();

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        try {
                            console.log("📥 Linha Processada da Pool:", line);
                            const poolData = JSON.parse(line);
                            
                            if (poolData && poolData.error) {
                                console.error(`❌ Erro da pool: [${poolData.error.code}] ${poolData.error.message}`);
                                ws.send(JSON.stringify({ identifier: "error", message: poolData.error.message }));
                                return;
                            }

                            // Conversão de Jobs para formato plano exigido pelo picojson
                            let rawJob = null;
                            if (poolData && poolData.result && poolData.result.job) {
                                rawJob = poolData.result.job;
                            } else if (poolData && poolData.method === "job") {
                                rawJob = poolData.params;
                            }

                            if (rawJob) {
                                const cplusplusJob = {
                                    identifier: "job",
                                    job_id: rawJob.job_id,
                                    blob: rawJob.blob,
                                    target: rawJob.target,
                                    height: Number(rawJob.height || 0),
                                    seed_hash: rawJob.seed_hash || ""
                                };
                                ws.send(JSON.stringify(cplusplusJob));
                                console.log("🎯 Job encapsulado enviado para o Worker Inline!");
                                return;
                            }

                            // Retorno de Handshake de Autenticação (Destrava wait_for do C++)
                            if (poolData && poolData.id === lastClientRpcId && poolData.result && typeof poolData.result === 'object' && poolData.result.id) {
                                ws.send(JSON.stringify({
                                    identifier: "handshake_reply",
                                    status: "authenticated",
                                    session: poolData.result.id
                                }));
                                console.log("🔑 Handshake validado e liberado para a Thread do C++.");
                                return;
                            }

                            // Confirmação de Share Aceito
                            if (poolData && poolData.result && poolData.result.status === "OK") {
                                ws.send(JSON.stringify({ identifier: "share_reply", status: "OK" }));
                                console.log("🔥 SUCESSO: Hash validado e aceito pela Pool!");
                                return;
                            }

                            if (poolData) ws.send(JSON.stringify(poolData));
                        } catch (e) {
                            console.error("❌ Falha ao processar JSON da Pool:", e.message);
                        }
                    });
                });

                stratumClient.on('error', (err) => console.error('❌ ERRO CRÍTICO NO SOCKET TCP DA POOL:', err.message));
                
                stratumClient.on('close', () => {
                    console.log('❌ Conexão com a Pool encerrada.');
                    poolConnected = false;
                });
            }

            // ENVIO DE SHARES RESOLVIDOS
            const isSubmit = data.method === 'submit' || data.identifier === 'submit';
            if (isSubmit && poolConnected && stratumClient?.writable) {
                const params = data.params || data;
                const stratumSubmit = {
                    id: lastClientRpcId,
                    method: "submit",
                    params: { 
                        id: params.id || params.job_id, 
                        job_id: params.job_id || params.id, 
                        nonce: params.nonce, 
                        result: params.result,
                        algo: "cn/lite" 
                    }
                };
                stratumClient.write(JSON.stringify(stratumSubmit) + "\n");
                console.log(`📤 Share enviado para a Pool.`);
            }
        } catch (err) {
            console.error('❌ Erro de processamento interno:', err.message);
        }
    });

    ws.on('close', () => { 
        console.log('🔌 Conexão WebSocket com o navegador fechada.');
        if (stratumClient) stratumClient.destroy(); 
    });
});

// Inicialização estável na interface global de rede
serverHttp.listen(PORT, '0.0.0.0', () => {
    console.log(`🌐 Servidor HTTP rodando na porta ${PORT}`);
});
