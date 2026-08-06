const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const cors = require('cors');

const PORT = process.env.PORT || 8080;

// 1. CRIAÇÃO DO SERVIDOR HTTP COM SUPORTE A CORS
const serverHttp = http.createServer((req, res) => {
    cors()(req, res, () => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Proxy MoneroOcean Ativo e Live!');
    });
});

// 2. CRIAÇÃO DO SERVIDOR WEBSOCKET
const wss = new WebSocket.Server({ server: serverHttp });
console.log(`🚀 Proxy Stratum ativo na porta ${PORT}`);

wss.on('connection', (ws) => {
    console.log('🔗 SINAL RECEBIDO: O navegador conectou com o Proxy!');
    let stratumClient = null;
    let poolConnected = false;
    let tcpBuffer = "";
    let lastClientRpcId = 1;

    // ESCUTA AS MENSAGENS VINDAS DO C++ (NAVEGADOR)
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📥 Mensagem do Navegador:', data.method || data.identifier || 'Dados crus');

            if (data.id) lastClientRpcId = data.id;

            // =================================================================
            // TRATAMENTO DE LOGIN / HANDSHAKE DINÂMICO
            // =================================================================
            if (data.method === 'login' || data.identifier === 'handshake' || data.identifier === 'login') {
                if (stratumClient) stratumClient.destroy();
                
                stratumClient = new net.Socket();
                
                // EXTRAÇÃO DINÂMICA: Captura a carteira e o worker enviados pelo HTML
                const params = data.params || {};
                const userWallet = params.login || data.wallet || "4657q4dnsjLWtzeW4XN3wG9swFumWAZB9i1pegTLMxVAQy5E5AE8uif42kkHWcWc9vDcLUmzeCf3pV7mmrJQQqqe84dtASi";
                const workerName = params.pass || data.worker || "WasmMiner";

                console.log(`⏳ Abrindo socket TCP com a carteira do usuário: ${userWallet}`);
                
                // Só abre o TCP agora, garantindo que o login vá junto imediatamente
                stratumClient.connect(10001, 'gulf.moneroocean.stream', () => {
                    console.log('✅ CONECTADO VIA TCP À POOL MONEROOCEAN!');
                    poolConnected = true;

                    const stratumLogin = {
                        id: lastClientRpcId,
                        method: "login",
                        params: {
                            login: userWallet,
                            pass: workerName,
                            agent: "XMR-RealRandomX/1.0"
                        }
                    };
                    
                    // Envia o login instantaneamente no milissegundo em que o TCP conecta
                    stratumClient.write(JSON.stringify(stratumLogin) + "\n");
                    console.log('📤 Login dinâmico enviado para a Pool.');
                });

                // =================================================================
                // PROCESSAMENTO DOS DADOS VINDOS DA POOL TCP -> NAVEGADOR (C++)
                // =================================================================
                stratumClient.on('data', (chunk) => {
                    tcpBuffer += chunk.toString();
                    let lines = tcpBuffer.split("\n");
                    tcpBuffer = lines.pop();

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        try {
                            console.log("📥 Linha Processada da Pool:", line);
                            const poolData = JSON.parse(line);
                            
                            if (poolData.error) {
                                console.error(`❌ Erro da pool: [${poolData.error.code}] ${poolData.error.message}`);
                                ws.send(JSON.stringify({ identifier: "error", message: poolData.error.message }));
                                return;
                            }

                            // 1. CAPTURA E CONVERSÃO DE JOBS PARA O FORMATO PLANO DO C++
                            let rawJob = null;
                            if (poolData.result && poolData.result.job) {
                                rawJob = poolData.result.job;
                            } else if (poolData.method === "job") {
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

                            // 2. RETORNO DE AUTENTICAÇÃO (Desbloqueia o wait_for do C++)
                            if (poolData.id === lastClientRpcId && poolData.result && poolData.result.id) {
                                ws.send(JSON.stringify({
                                    identifier: "handshake_reply",
                                    status: "authenticated",
                                    session: poolData.result.id
                                }));
                                console.log("🔑 Handshake validado e liberado para a Thread do C++.");
                                return;
                            }

                            // 3. CONFIRMAÇÃO DE SHARE ACEITO
                            if (poolData.result && poolData.result.status === "OK") {
                                ws.send(JSON.stringify({ identifier: "share_reply", status: "OK" }));
                                console.log("🔥 SUCESSO: Hash validado e aceito pela Pool!");
                                return;
                            }

                            ws.send(JSON.stringify(poolData));

                        } catch (e) {
                            console.error("❌ Falha ao processar JSON da Pool:", e.message);
                        }
                    });
                });

                stratumClient.on('error', (err) => console.error('❌ Erro no socket TCP:', err.message));
                stratumClient.on('close', () => {
                    console.log('❌ Conexão com a Pool encerrada.');
                    poolConnected = false;
                });
            }

            // =================================================================
            // TRATAMENTO DE ENVIO DE SHARE (C++ -> PROXY -> POOL)
            // =================================================================
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
                        algo: "rx/0"
                    }
                };
                stratumClient.write(JSON.stringify(stratumSubmit) + "\n");
                console.log(`📤 Compartilhamento (Share ID: ${params.job_id || params.id}) enviado para a Pool.`);
            }
        } catch (err) {
            console.error('❌ Erro no processamento do WebSocket:', err.message);
        }
    });

    ws.on('close', () => { 
        console.log('🔌 Conexão WebSocket com o navegador fechada.');
        if (stratumClient) stratumClient.destroy(); 
    });
});

serverHttp.listen(PORT, () => {
    console.log(`🌐 Servidor HTTP rodando de forma estável na porta ${PORT}`);
});
