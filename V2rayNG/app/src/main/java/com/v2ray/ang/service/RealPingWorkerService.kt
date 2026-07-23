package com.v2ray.ang.service

import android.content.Context
import com.v2ray.ang.core.CoreConfigManager
import com.v2ray.ang.core.CoreNativeManager
import com.v2ray.ang.dto.RealPingEvent
import com.v2ray.ang.enums.EConfigType
import com.v2ray.ang.extension.isComplexType
import com.v2ray.ang.extension.isNotNullEmpty
import com.v2ray.ang.handler.MmkvManager
import com.v2ray.ang.handler.SettingsManager
import com.v2ray.ang.handler.SpeedtestManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Worker that runs a batch of real-ping tests independently.
 * Each batch owns its own CoroutineScope/dispatcher and can be cancelled separately.
 */
class RealPingWorkerService(
    private val context: Context,
    private val guids: List<String>,
    private val onEvent: (RealPingEvent) -> Unit = {}
) {
    private val job = SupervisorJob()
    // Every measureOutboundDelay call creates a temporary Xray core. A large parallel burst
    // overloads the shared ciadpi listener and produces false TLS timeouts/closed pipes.
    private val concurrency = if (com.v2ray.ang.dpi.ByeDpiManager.isRunning()) {
        SettingsManager.getRealPingConcurrency().coerceAtMost(8)
    } else {
        SettingsManager.getRealPingConcurrency()
    }
    private val dispatcher = Executors.newFixedThreadPool(concurrency).asCoroutineDispatcher()
    private val scope = CoroutineScope(job + dispatcher + CoroutineName("RealPingBatchWorker"))

    private val runningCount = AtomicInteger(0)
    private val totalCount = AtomicInteger(0)

    fun start() {
        val jobs = guids.map { guid ->
            totalCount.incrementAndGet()
            scope.launch {
                runningCount.incrementAndGet()
                try {
                    val result = startRealPing(guid)
                    onEvent(RealPingEvent.Result(guid, result))
                } catch (_: Throwable) {
                    // ignore
                } finally {
                    val count = totalCount.decrementAndGet()
                    val left = runningCount.decrementAndGet()
                    onEvent(RealPingEvent.Progress("$left / $count"))
                }
            }
        }

        scope.launch {
            try {
                joinAll(*jobs.toTypedArray())
                onEvent(RealPingEvent.Finish("0"))
            } catch (_: CancellationException) {
                onEvent(RealPingEvent.Finish("-1"))
            } finally {
                close()
            }
        }
    }

    fun cancel() {
        job.cancel()
    }

    private fun close() {
        try {
            dispatcher.close()
        } catch (_: Throwable) {
            // ignore
        }
    }

    private suspend fun startRealPing(guid: String): Long {
        val retFailure = -1L
        val dpiRunning = com.v2ray.ang.dpi.ByeDpiManager.isRunning()

        val config = MmkvManager.decodeServerConfig(guid) ?: return retFailure
        // A direct TCP pre-check bypasses ciadpi. Under DPI filtering it can reject a profile
        // that succeeds when launched through the real ByeDPI chain. Keep it only for normal mode.
        if (!dpiRunning
            && !config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.startsWith("h3") != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val url = config.server.orEmpty()
            val port = config.serverPort.orEmpty().toInt()
            val tcpTime = SpeedtestManager.socketConnectTime(url, port, 3000)
            if (tcpTime <= -1L) return retFailure
        }

        val configResult = CoreConfigManager.getV2rayConfig4Speedtest(context, guid)
        if (!configResult.status) return retFailure

        val urls = if (dpiRunning) {
            listOf(SettingsManager.getDelayTestUrl(), SettingsManager.getDelayTestUrl(true))
        } else {
            listOf(SettingsManager.getDelayTestUrl())
        }.distinct()

        urls.forEachIndexed { index, url ->
            val result = CoreNativeManager.measureOutboundDelay(configResult.content, url)
            if (result >= 0L) return result
            if (dpiRunning && index + 1 < urls.size) delay(450L)
        }
        return retFailure
    }
}
