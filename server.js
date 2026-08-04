const WebSocket = require('ws');
const net = require('net');
const http = require('http');
const cors = require('cors');

const PORT = process.env.PORT || 8080;

const serverHttp = http.createServer((req, res) => {
    cors()(req, res, () => {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Proxy MoneroOcean Ativo e Live!');
    });
});

const wss = new WebSocket.Server({ server: serverHttp });
console.log(`🚀 Proxy Stratum ativo na porta ${PORT}`);

wss.on('connection', (ws) => {
    console.log('🔗 SINAL RECEBIDO: O navegador conectou com o Proxy!');
    let stratumClient = null;
    let poolConnected = false;
    let tcpBuffer = "";
    let lastClientRpcId = 1; // Guarda o ID original para responder à thread C++

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            console.log('📥 Mensagem do Navegador:', data.method || data.identifier || 'Dados crus');

            if (data.id) lastClientRpcId = data.id;

            // 1. TRATAMENTO DE LOGIN (Nativo JSON-RPC ou Customizado)
            if (data.method === 'login' || data.identifier === 'handshake' || data.identifier === 'login') {
                if (stratumClient) stratumClient.destroy();
                stratumClient = new net.Socket();
                
                console.log('⏳ Tentando abrir socket TCP com a Pool MoneroOcean...');
                
                stratumClient.connect(10001, 'gulf.moneroocean.stream', () => {
                    console.log('✅ CONECTADO VIA TCP À POOL MONEROOCEAN!');
                    poolConnected = true;

                    // Captura os parâmetros conforme a estrutura enviada pelo C++ ou fallback
                    const params = data.params || {};
                    let userWallet = params.login || data.wallet || "4657q4dnsjLWtzeW4XN3wG9swFumWAZB9i1pegTLMxVAQy5E5AE8uif42kkHWcWc9vDcLUmzeCf3pV7mmrJQQqqe84dtASi";

                    const stratumLogin = {
                        id: lastClientRpcId,
                        method: "login",
                        params: {
                            login: userWallet,
                            pass: params.pass || "x"
                            agent: params.agent || "XMR-CryptoNightWeb/1.0",
                        }
                    };
                    
                    stratumClient.write(JSON.stringify(stratumLogin) + "\n");
                    console.log('📤 Login enviado para a Pool:', userWallet);
                });

                stratumClient.on('data', (chunk) => {
                    tcpBuffer += chunk.toString();
                    let lines = tcpBuffer.split("\n");
                    tcpBuffer = lines.pop();

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        try {
                            console.log("📥 Linha Processada da Pool:", line);
                            const poolData = JSON.parse(line);
                            
                            // Erro retornado pela pool -> Envelopa de volta em JSON-RPC
                            if (poolData.error) {
                                console.error(`❌ Erro da pool: [${poolData.error.code}] ${poolData.error.message}`);
                                ws.send(JSON.stringify({
                                    jsonrpc: "2.0",
                                    id: poolData.id || lastClientRpcId,
                                    error: poolData.error
                                }));
                                return;
                            }

                            // Captura e Padroniza mensagens de "job" vindas da Pool
                            if (poolData.result && poolData.result.job) {
                                ws.send(JSON.stringify({
                                    jsonrpc: "2.0",
                                    method: "job",
                                    params: poolData.result.job
                                }));
                            } else if (poolData.method === "job") {
                                ws.send(JSON.stringify({
                                    jsonrpc: "2.0",
                                    method: "job",
                                    params: poolData.params
                                }));
                            } else if (poolData.result && poolData.result.status === "OK") {
                                // Confirmação de Share Aceito
                                ws.send(JSON.stringify({
                                    jsonrpc: "2.0",
                                    id: poolData.id || lastClientRpcId,
                                    result: { status: "OK" },
                                    error: null
                                }));
                                console.log("🔥 SUCESSO: Hash validado e aceito pela Pool!");
                            } else {
                                // Encaminha qualquer outra resposta genérica mantendo a integridade do JSON
                                ws.send(JSON.stringify(poolData));
                            }
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

            // 2. TRATAMENTO DE ENVIO DE SHARE (Nativo JSON-RPC ou Customizado)
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
                console.log(`📤 Compartilhamento (Share ID: ${params.job_id || params.id}) enviado para a Pool.`);
            }
        } catch (err) {
            console.error('❌ Erro no processamento do WebSocket:', err.message);
        }
    });

    ws.on('close', () => { if (stratumClient) stratumClient.destroy(); });
});

serverHttp.listen(PORT);
