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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
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

    private suspend fun runBatch() = coroutineScope {
        if (guids.isEmpty()) return@coroutineScope

        val dpiRunning = ByeDpiManager.isRunning()
        val configuredConcurrency = SettingsManager.getRealPingConcurrency()
        val concurrency = if (dpiRunning) {
            configuredConcurrency.coerceIn(4, 10)
        } else {
            configuredConcurrency.coerceIn(1, 64)
        }.coerceAtMost(guids.size)

        if (dpiRunning) delay(250L)

        // A worker pool emits each result immediately. The previous wave barrier waited for the
        // slowest profile before updating progress, which looked frozen at 1 / 1 for minutes.
        val nextIndex = AtomicInteger(0)
        repeat(concurrency) { workerIndex ->
            launch {
                if (dpiRunning && workerIndex > 0) delay((workerIndex * 35L).coerceAtMost(250L))
                while (true) {
                    coroutineContext.ensureActive()
                    val index = nextIndex.getAndIncrement()
                    if (index >= guids.size) break

                    val result = testProfile(guids[index], dpiRunning)
                    onEvent(RealPingEvent.Result(result.guid, result.delayMillis))
                    val done = completedCount.incrementAndGet()
                    onEvent(RealPingEvent.Progress("$done / ${guids.size}"))

                    if (dpiRunning && result.transientFailure) delay(80L)
                }
            }
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
        // One primary request plus at most one fallback request. Three rounds over two URLs made
        // four blocked profiles hold an entire wave for up to a minute.
        for ((index, url) in urls.take(if (dpiRunning) 2 else 1).withIndex()) {
            coroutineContext.ensureActive()
            val measurement = CoreNativeManager.measureOutboundDelayDetailed(configResult.content, url)
            if (measurement.delayMillis >= 0L) {
                return PingResult(guid, measurement.delayMillis, transientFailure = false)
            }
            if (!measurement.isTransient) return failure
            sawTransientFailure = true
            if (index == 0 && urls.size > 1) delay(120L + Random.nextLong(20L, 100L))
        }
        return PingResult(guid, -1L, transientFailure = sawTransientFailure)
    }
}
