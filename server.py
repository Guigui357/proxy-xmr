import asyncio
import json
import os
import sys

# Configuração de Porta dinamicamente compatível com o ambiente local e Render
PORT = int(os.environ.get("PORT", 8080))
POOL_HOST = "gulf.moneroocean.stream"
POOL_PORT = 443  # Usando a porta 443 para mascarar o tráfego e burlar firewalls

async def handle_stratum_to_cplusplus(pool_reader, ws):
    """Captura os dados vindos da Pool via TCP, trata e repassa para o C++ via WebSocket"""
    buffer = ""
    try:
        while True:
            chunk = await pool_reader.read(4096)
            if not chunk:
                print("❌ Conexão com a Pool encerrada pela própria MoneroOcean.")
                break
                
            buffer += chunk.decode('utf-8', errors='ignore')
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                    
                try:
                    print(f"📥 Linha Processada da Pool: {line.strip()}")
                    pool_data = json.loads(line)
                    
                    if not pool_data:
                        continue

                    # 1. TRATAMENTO DE ERROS DA POOL
                    if "error" in pool_data and pool_data["error"]:
                        print(f"❌ Erro da pool: {pool_data['error'].get('message')}")
                        await ws.send(json.dumps({
                            "identifier": "error",
                            "message": pool_data["error"].get("message", "Erro desconhecido")
                        }))
                        continue

                    # 2. CAPTURA E CONVERSÃO DE JOBS PARA O FORMATO PLANO DO C++ (picojson)
                    raw_job = None
                    if "result" in pool_data and pool_data["result"] and "job" in pool_data["result"]:
                        raw_job = pool_data["result"]["job"]
                    elif pool_data.get("method") == "job":
                        raw_job = pool_data.get("params")

                    if raw_job:
                        cplusplus_job = {
                            "identifier": "job",
                            "job_id": raw_job.get("job_id"),
                            "blob": raw_job.get("blob"),
                            "target": raw_job.get("target"),
                            "height": int(raw_job.get("height", 0)),
                            "seed_hash": raw_job.get("seed_hash", "")
                        }
                        await ws.send(json.dumps(cplusplus_job))
                        print("🎯 Job encapsulado enviado para o navegador!")
                        continue

                    # 3. RETORNO DE AUTENTICAÇÃO (Desbloqueia o wait_for do C++)
                    if "result" in pool_data and isinstance(pool_data["result"], dict) and "id" in pool_data["result"]:
                        await ws.send(json.dumps({
                            "identifier": "handshake_reply",
                            "status": "authenticated",
                            "session": pool_data["result"]["id"]
                        }))
                        print("🔑 Handshake validado e liberado para a Thread do C++.")
                        continue

                    # 4. CONFIRMAÇÃO DE SHARE ACEITO
                    if "result" in pool_data and pool_data["result"] and pool_data["result"].get("status") == "OK":
                        await ws.send(json.dumps({"identifier": "share_reply", "status": "OK"}))
                        print("🔥 SUCESSO: Hash validado e aceito pela Pool!")
                        continue

                    # Encaminhamento íntegro de contingência
                    await ws.send(json.dumps(pool_data))

                except json.JSONDecodeError:
                    print("❌ Falha ao processar JSON da Pool.")
    except Exception as e:
        print(f"⚠️ Exceção na leitura do canal TCP da Pool: {e}")

async def proxy_handler(ws, path=None):
    """Gerencia a sessão do WebSocket vinda do navegador de forma irrestrita"""
    print("🔗 SINAL RECEBIDO: O navegador conectou com o Proxy Python!")
    
    pool_reader = None
    pool_writer = None
    pool_task = None
    pool_connected = False
    last_rpc_id = 1

    try:
        async for message in ws:
            try:
                data = json.loads(message)
                print(f"📥 Mensagem do Navegador: {data.get('method') or data.get('identifier') or 'Dados crus'}")

                if "id" in data:
                    last_rpc_id = int(data["id"])

                # =================================================================
                # CAPTURA DE LOGIN / HANDSHAKE DINÂMICO
                # =================================================================
                if data.get("method") == "login" or data.get("identifier") == "handshake" or data.get("identifier") == "login":
                    if pool_writer:
                        pool_writer.close()
                        await pool_writer.wait_closed()

                    params = data.get("params", {})
                    user_wallet = params.get("login") or data.get("wallet") or "4657q4dnsjLWtzeW4XN3wG9swFumWAZB9i1pegTLMxVAQy5E5AE8uif42kkHWcWc9vDcLUmzeCf3pV7mmrJQQqqe84dtASi"
                    worker_name = params.get("pass") or data.get("worker") or "WasmMiner"

                    print(f"⏳ [PROCESSO] Iniciando canal TCP. Carteira: {user_wallet[:12]}...")

                    try:
                        # Abre o Socket TCP Assíncrono com a Pool MoneroOcean
                        pool_reader, pool_writer = await asyncio.open_connection(POOL_HOST, POOL_PORT)
                        print("✅ CONECTADO VIA TCP À POOL MONEROOCEAN!")
                        pool_connected = True

                        # Dispara a tarefa paralela para ler as respostas da Pool
                        pool_task = asyncio.create_task(handle_stratum_to_cplusplus(pool_reader, ws))

                        # Envia o login no formato estrito exigido pelo Stratum
                        stratum_login = {
                            "id": last_rpc_id,
                            "method": "login",
                            "params": {
                                "login": user_wallet,
                                "pass": worker_name,
                                "agent": "XMR-CryptoNightWeb/1.0"
                            }
                        }
                        payload = json.dumps(stratum_login) + "\n"
                        pool_writer.write(payload.encode('utf-8'))
                        await pool_writer.drain()
                        print("📤 Login Stratum enviado para a Pool.")

                    except Exception as tcp_err:
                        print(f"❌ ERRO CRÍTICO NO SOCKET TCP DA POOL: {tcp_err}")
                        await ws.send(json.dumps({"identifier": "error", "message": f"Falha TCP: {tcp_err}"}))

                # =================================================================
                # TRATAMENTO DE ENVIO DE SHARE (C++ -> PROXY -> POOL)
                # =================================================================
                is_submit = data.get("method") == "submit" or data.get("identifier") == "submit"
                if is_submit and pool_connected and pool_writer and not pool_writer.is_closing():
                    params = data.get("params") or data
                    stratum_submit = {
                        "id": last_rpc_id,
                        "method": "submit",
                        "params": {
                            "id": params.get("id") or params.get("job_id"),
                            "job_id": params.get("job_id") or params.get("id"),
                            "nonce": params.get("nonce"),
                            "result": params.get("result"),
                            "algo": params.get("algo") or "rx/0"  # Garanta rx/0 para RandomX
                        }
                    }
                    payload = json.dumps(stratum_submit) + "\n"
                    pool_writer.write(payload.encode('utf-8'))
                    await pool_writer.drain()
                    print("📤 Share enviado para a Pool.")

            except json.JSONDecodeError:
                print("❌ Falha ao processar payload vindo do navegador (JSON Inválido).")
            except Exception as inner_err:
                print(f"⚠️ Erro no laço de processamento de mensagem: {inner_err}")

    except Exception as ws_err:
        print(f"⚠️ Erro na sessão do WebSocket: {ws_err}")
    finally:
        print("🔌 Conexão WebSocket com o navegador fechada.")
        if pool_task:
            pool_task.cancel()
        if pool_writer:
            pool_writer.close()
            try:
                await pool_writer.wait_closed()
            except:
                pass

async def main():
    import websockets
    
    # Interceptação permissiva de sub-protocolos para evitar o TypeError no Python 3.9+
    async with websockets.serve(
        proxy_handler, 
        "0.0.0.0", 
        PORT,
        subprotocols=["binary", "base64"]
    ):
        print(f"🚀 Proxy Stratum em Python ativo na porta {PORT}")
        await asyncio.Future()  # Executa indefinidamente

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Servidor encerrado pelo usuário.")
        sys.exit(0)
