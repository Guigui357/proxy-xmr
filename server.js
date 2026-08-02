const WebSocket = require('ws');
const net = require('net');

// Porta padrão onde o proxy vai escutar o seu site do Netlify
const PORT = 8080; 

const wss = new WebSocket.Server({ port: PORT });
console.log(`Servidor Proxy WebSocket rodando na porta ${PORT}`);

wss.on('connection', (ws) => {
    console.log('Navegador conectado ao Proxy!');

    // Abre uma conexão TCP Socket direto com a Pool da MoneroOcean
    const stratumClient = new net.Socket();
    
    // Conecta na URL padrão da MoneroOcean via protocolo Stratum
    stratumClient.connect(18081, 'gulf.moneroocean.stream', () => {
        console.log('Proxy conectado com sucesso à Pool MoneroOcean!');
    });

    // Quando o navegador enviar dados de hash, o proxy repassa para a Pool
    ws.on('message', (message) => {
        if (stratumClient.writable) {
            stratumClient.write(message + '\n');
        }
    });

    // Quando a Pool responder com novos trabalhos (Jobs), o proxy repassa para o navegador
    stratumClient.on('data', (data) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(data.toString());
        }
    });

    // Tratamento de erros e desconexões
    ws.on('close', () => stratumClient.destroy());
    stratumClient.on('close', () => ws.close());
    ws.on('error', () => stratumClient.destroy());
    stratumClient.on('error', () => ws.close());
});
