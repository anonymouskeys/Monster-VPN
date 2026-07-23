package com.v2ray.ang.service

import android.content.Context
import com.v2ray.ang.AppConfig
import com.v2ray.ang.core.CoreConfigManager
import com.v2ray.ang.core.CoreNativeManager
import com.v2ray.ang.dpi.ByeDpiManager
import com.v2ray.ang.dto.RealPingEvent
import com.v2ray.ang.enums.EConfigType
import com.v2ray.ang.extension.isComplexType
import com.v2ray.ang.extension.isNotNullEmpty
import com.v2ray.ang.handler.MmkvManager
import com.v2ray.ang.handler.SettingsManager
import com.v2ray.ang.handler.SpeedtestManager
import com.v2ray.ang.util.LogUtil
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

/**
 * Runs one cancellable batch of real-ping tests.
 *
 * DPI mode deliberately uses adaptive waves instead of launching one temporary Xray core per
 * configured executor thread forever. A healthy ciadpi listener is allowed to ramp up quickly;
 * EOF/closed-pipe/TLS-timeout bursts immediately reduce pressure and only the affected profiles
 * are retried. This keeps large (thousands of profiles) batches fast without turning temporary
 * listener overload into persistent -1 ms results.
 */
class RealPingWorkerService(
    private val context: Context,
    private val guids: List<String>,
    private val onEvent: (RealPingEvent) -> Unit = {}
) {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(job + Dispatchers.IO + CoroutineName("RealPingBatchWorker"))
    private val completedCount = AtomicInteger(0)

    private data class PingResult(
        val guid: String,
        val delayMillis: Long,
        val transientFailure: Boolean
    )

    fun start() {
        scope.launch {
            try {
                runBatch()
                onEvent(RealPingEvent.Finish("0"))
            } catch (_: CancellationException) {
                onEvent(RealPingEvent.Finish("-1"))
            }
        }
    }

    fun cancel() {
        job.cancel()
    }

    private suspend fun runBatch() {
        if (guids.isEmpty()) return

        val dpiRunning = ByeDpiManager.isRunning()
        val configuredConcurrency = SettingsManager.getRealPingConcurrency()
        val maximumConcurrency = if (dpiRunning) {
            // ciadpi can sustain parallel work, but an unbounded burst of temporary Xray cores
            // is what produced the closed-pipe storm seen in device logs.
            configuredConcurrency.coerceIn(2, 12)
        } else {
            configuredConcurrency.coerceIn(1, 64)
        }
        var currentConcurrency = if (dpiRunning) min(4, maximumConcurrency) else maximumConcurrency
        var cursor = 0

        if (dpiRunning) {
            // acquire() already verifies the listener, but a short quiet period prevents the first
            // wave from racing native process startup on slower devices.
            delay(250L)
        }

        while (cursor < guids.size) {
            coroutineContext.ensureActive()
            val end = min(cursor + currentConcurrency, guids.size)
            val wave = guids.subList(cursor, end)
            val results = wave.mapIndexed { index, guid ->
                scope.async {
                    // Do not make every temporary core hit ciadpi in the same millisecond.
                    if (dpiRunning && index > 0) delay(Random.nextLong(20L, 90L))
                    testProfile(guid, dpiRunning)
                }
            }.awaitAll()

            results.forEach { result ->
                onEvent(RealPingEvent.Result(result.guid, result.delayMillis))
                val done = completedCount.incrementAndGet()
                onEvent(RealPingEvent.Progress("$done / ${guids.size}"))
            }

            if (dpiRunning) {
                val transientFailures = results.count { it.transientFailure }
                val failureRatio = transientFailures.toDouble() / results.size.coerceAtLeast(1)
                val oldConcurrency = currentConcurrency
                currentConcurrency = when {
                    failureRatio >= 0.50 -> max(2, currentConcurrency / 2)
                    failureRatio >= 0.25 -> max(2, currentConcurrency - 1)
                    failureRatio == 0.0 -> min(maximumConcurrency, currentConcurrency + 2)
                    failureRatio <= 0.10 -> min(maximumConcurrency, currentConcurrency + 1)
                    else -> currentConcurrency
                }
                if (oldConcurrency != currentConcurrency) {
                    LogUtil.i(
                        AppConfig.TAG,
                        "RealPing DPI concurrency $oldConcurrency -> $currentConcurrency " +
                            "(transient=$transientFailures/${results.size})"
                    )
                }
                // Let ciadpi drain sockets after an unhealthy wave; healthy waves continue nearly
                // immediately, preserving throughput for 5000+ profile lists.
                if (transientFailures > 0) delay(180L) else delay(25L)
            }
            cursor = end
        }
    }

    private suspend fun testProfile(guid: String, dpiRunning: Boolean): PingResult {
        val failure = PingResult(guid, -1L, transientFailure = false)
        val config = MmkvManager.decodeServerConfig(guid) ?: return failure

        // A direct TCP pre-check bypasses ByeDPI. Keep it only for normal mode, otherwise it can
        // reject a profile that succeeds through the actual DPI chain.
        if (!dpiRunning
            && !config.configType.isComplexType()
            && config.configType != EConfigType.HYSTERIA2
            && config.configType != EConfigType.WIREGUARD
            && config.alpn?.startsWith("h3") != true
            && config.server.isNotNullEmpty()
            && config.serverPort?.toIntOrNull() != null
        ) {
            val tcpTime = SpeedtestManager.socketConnectTime(
                config.server.orEmpty(),
                config.serverPort.orEmpty().toInt(),
                3000
            )
            if (tcpTime <= -1L) return failure
        }

        val configResult = CoreConfigManager.getV2rayConfig4Speedtest(context, guid)
        if (!configResult.status) return failure

        val urls = if (dpiRunning) {
            listOf(SettingsManager.getDelayTestUrl(), SettingsManager.getDelayTestUrl(true)).distinct()
        } else {
            listOf(SettingsManager.getDelayTestUrl())
        }

        var sawTransientFailure = false
        val maxAttempts = if (dpiRunning) 3 else 1
        repeat(maxAttempts) { attempt ->
            coroutineContext.ensureActive()
            for (url in urls) {
                val measurement = CoreNativeManager.measureOutboundDelayDetailed(configResult.content, url)
                if (measurement.delayMillis >= 0L) {
                    return PingResult(guid, measurement.delayMillis, transientFailure = false)
                }
                if (!measurement.isTransient) {
                    // Certificate/SNI/configuration errors will not become healthy by retrying and
                    // should not slow a 5000-profile batch.
                    return failure
                }
                sawTransientFailure = true
            }
            if (attempt + 1 < maxAttempts) {
                delay(250L * (attempt + 1) + Random.nextLong(40L, 160L))
            }
        }
        return PingResult(guid, -1L, transientFailure = sawTransientFailure)
    }
}
