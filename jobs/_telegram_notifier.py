# Databricks notebook source
# =============================================================================
# jobs/_telegram_notifier.py
#
# Notificador reutilizável via Telegram Bot API. Incluído nos jobs via:
#   %run ./_telegram_notifier
#
# Nunca deixa uma falha de notificação derrubar o pipeline — envio é sempre
# best-effort, com try/except isolado. Credenciais vêm exclusivamente de
# dbutils.secrets — nunca hardcoded aqui.
# =============================================================================

import datetime


class TelegramNotifier:

    def __init__(self, secret_scope: str, token_key: str, chat_id_key: str, enabled: bool = True):
        self.enabled = enabled
        self.token = None
        self.chat_id = None
        if not self.enabled:
            return
        try:
            self.token = dbutils.secrets.get(scope=secret_scope, key=token_key)
            self.chat_id = dbutils.secrets.get(scope=secret_scope, key=chat_id_key)
        except Exception as exc:
            # Se o secret não existir ainda, o pipeline continua rodando normalmente
            # sem notificação — só avisa no log do driver.
            print(f"[telegram] secret indisponível ({exc}) — notificações desativadas nesta execução.")
            self.enabled = False

    def _send(self, text: str):
        if not self.enabled:
            return
        try:
            import requests
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            resp = requests.post(
                url,
                data={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as exc:
            # Notificação nunca pode ser a causa de o Job falhar.
            print(f"[telegram] falha ao enviar notificação: {exc}")

    @staticmethod
    def _icone(status: str) -> str:
        return {"SUCCESS": "✅", "FAILED": "❌", "PARTIAL": "⚠️"}.get(status, "•")

    def notify_run_summary(self, job_name: str, run_id: str, resultados: list[dict],
                            start_time: datetime.datetime, end_time: datetime.datetime):
        """
        Envia UMA mensagem consolidada por execução, com resumo por coleção.
        `resultados` é uma lista de dicts com pelo menos:
            collection, status, load_type, qtd_lida, mensagem_erro (opcional)
        """
        if not self.enabled:
            return

        houve_falha = any(r["status"] == "FAILED" for r in resultados)
        status_geral = "🔴 *FALHA*" if houve_falha else "🟢 *SUCESSO*"
        duracao = (end_time - start_time).total_seconds()

        linhas = [f"*{job_name}* — {status_geral}", f"`run_id={run_id[:8]}...`",
                  f"início: {start_time:%Y-%m-%d %H:%M UTC} | duração: {duracao:.1f}s", ""]

        for r in resultados:
            icone = self._icone(r["status"])
            linha = f"{icone} `{r['collection']}` ({r.get('load_type', '?')}): {r.get('qtd_lida', 0)} registros"
            if r["status"] == "FAILED" and r.get("mensagem_erro"):
                erro_curto = str(r["mensagem_erro"])[:200]
                linha += f"\n     ↳ _{erro_curto}_"
            linhas.append(linha)

        self._send("\n".join(linhas))

    def notify_failure(self, job_name: str, run_id: str, erro: str):
        """Notificação isolada para uma falha inesperada que interrompeu o job inteiro
        antes de chegar ao resumo por coleção (ex.: erro de conexão logo no início)."""
        if not self.enabled:
            return
        texto = (
            f"🔴 *{job_name} — FALHA CRÍTICA*\n"
            f"`run_id={run_id[:8]}...`\n"
            f"{str(erro)[:500]}"
        )
        self._send(texto)
